"""
Extract all geotagged Wikipedia articles from SQL dumps into GeoParquet.

Streams these dumps from dumps.wikimedia.org (~12GB total):
  - geo_tags:   coordinates for geotagged pages (~52MB)
  - page:       page metadata — title, length (~2.4GB)
  - page_props: lead images, Wikidata QIDs, descriptions (~1GB)
  - linktarget: link target ID mappings (~1.4GB)
  - pagelinks:  internal links for inlink counts (~6.9GB)

Outputs a Hilbert-sorted GeoParquet 1.1 file with bbox covering metadata.
Use --test to validate with a small subset first.
"""

import argparse
import gzip
import os
import re
import sys
import time
import urllib.request

sys.stdout.reconfigure(line_buffering=True)

DUMP_BASE_URL = "https://dumps.wikimedia.org/enwiki/latest"
DUMP_LOCAL_DIR = "data/dumps"
OUTPUT_FILE = "data/wikipedia_geotagged.parquet"
USER_AGENT = "wiki-geoparquet/1.0 (github.com/Shane98c/wiki-geoparquet)"

COP_DEM_BASE = "https://copernicus-dem-30m.s3.amazonaws.com"

DUMP_FILES = {
    "geo_tags":   "enwiki-latest-geo_tags.sql.gz",
    "page":       "enwiki-latest-page.sql.gz",
    "page_props": "enwiki-latest-page_props.sql.gz",
    "linktarget": "enwiki-latest-linktarget.sql.gz",
    "pagelinks":  "enwiki-latest-pagelinks.sql.gz",
}


# ── MySQL dump parser ─────────────────────────────────────────

def parse_insert_tuples(line_bytes, table_name):
    """Parse MySQL INSERT statement, yielding tuples of values.

    Handles: quoted strings (with \\-escapes), integers, floats, NULL.
    """
    prefix = f"INSERT INTO `{table_name}` VALUES ".encode()
    if not line_bytes.startswith(prefix):
        return

    data = line_bytes[len(prefix):]
    pos = 0
    n = len(data)

    while pos < n:
        if data[pos:pos + 1] != b'(':
            pos += 1
            continue

        pos += 1  # skip '('
        values = []

        while pos < n:
            ch = data[pos:pos + 1]

            if ch == b"'":
                # Quoted string
                pos += 1
                parts = []
                while pos < n:
                    c = data[pos:pos + 1]
                    if c == b'\\':
                        pos += 1
                        esc = data[pos:pos + 1]
                        if esc == b'n':
                            parts.append(b'\n')
                        elif esc == b't':
                            parts.append(b'\t')
                        elif esc == b'r':
                            parts.append(b'\r')
                        elif esc == b'0':
                            parts.append(b'\x00')
                        elif esc == b'b':
                            parts.append(b'\x08')
                        elif esc == b'Z':
                            parts.append(b'\x1a')
                        else:
                            parts.append(esc)  # handles \\, \', \"
                        pos += 1
                    elif c == b"'":
                        pos += 1
                        break
                    else:
                        parts.append(c)
                        pos += 1
                values.append(b''.join(parts).decode('utf-8', errors='replace'))

            elif data[pos:pos + 4] == b'NULL':
                values.append(None)
                pos += 4

            else:
                # Number or unquoted value
                end = pos
                while end < n and data[end:end + 1] not in (b',', b')'):
                    end += 1
                token = data[pos:end]
                try:
                    if b'.' in token:
                        values.append(float(token))
                    else:
                        values.append(int(token))
                except ValueError:
                    values.append(token.decode('ascii', errors='replace'))
                pos = end

            if pos < n and data[pos:pos + 1] == b',':
                pos += 1
            elif pos < n and data[pos:pos + 1] == b')':
                pos += 1
                break

        yield tuple(values)

        # Skip separators between tuples
        while pos < n and data[pos:pos + 1] in (b',', b'\n', b'\r', b';', b' '):
            pos += 1


def stream_dump(name, table_name, max_insert_lines=None):
    """Stream a MySQL dump, yielding parsed tuples.

    Uses local file from data/dumps/ if available, otherwise streams from URL.
    """
    filename = DUMP_FILES[name]
    local_path = os.path.join(DUMP_LOCAL_DIR, filename)

    if os.path.exists(local_path):
        print(f"  Reading local: {local_path}")
        gz = gzip.open(local_path, 'rb')
    else:
        url = f"{DUMP_BASE_URL}/{filename}"
        print(f"  Streaming: {url}")
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        resp = urllib.request.urlopen(req)
        gz = gzip.GzipFile(fileobj=resp)

    insert_lines = 0
    for line_bytes in gz:
        if not line_bytes.startswith(b'INSERT'):
            continue

        insert_lines += 1
        if max_insert_lines and insert_lines > max_insert_lines:
            break

        yield from parse_insert_tuples(line_bytes, table_name)

    gz.close()


# ── Processing steps ──────────────────────────────────────────

def step1_geo_tags(test_mode):
    """Extract all geotagged pages on Earth."""
    print("\n→ Step 1/5: Streaming geo_tags (~52MB)...")
    geo_pages = {}
    total = 0
    limit = 50 if test_mode else None

    for t in stream_dump("geo_tags", "geo_tags", max_insert_lines=limit):
        total += 1
        # Columns:
        #   0: gt_id, 1: gt_page_id, 2: gt_globe, 3: gt_primary,
        #   4: gt_lat, 5: gt_lon, 6: gt_dim, 7: gt_type,
        #   8: gt_name, 9: gt_country, 10: gt_region
        if len(t) < 6:
            continue

        page_id = t[1]
        globe = t[2]
        primary = t[3]
        lat = t[4]
        lon = t[5]
        gt_type = t[7] if len(t) > 7 else ""

        # Filter: earth coordinates with valid ranges
        if globe != 'earth':
            continue
        if not (isinstance(lat, (int, float)) and isinstance(lon, (int, float))):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue

        # Prefer primary coords; accept non-primary if it's the only entry
        if page_id in geo_pages:
            if geo_pages[page_id]["gt_primary"]:
                continue  # already have a primary coord, skip
            if primary == 1:
                # upgrade to primary
                geo_pages[page_id] = {
                    "lat": float(lat),
                    "lon": float(lon),
                    "gt_type": gt_type or "",
                    "gt_primary": True,
                }
        else:
            geo_pages[page_id] = {
                "lat": float(lat),
                "lon": float(lon),
                "gt_type": gt_type or "",
                "gt_primary": primary == 1,
            }

    primary_count = sum(1 for p in geo_pages.values() if p["gt_primary"])
    print(f"  Scanned {total:,} geo_tags → {len(geo_pages):,} pages ({primary_count:,} primary, {len(geo_pages) - primary_count:,} non-primary)")
    return geo_pages


def step2_page(geo_pages, test_mode):
    """Get page titles, lengths; filter out redirects and non-articles."""
    print(f"\n→ Step 2/5: Streaming page (~2.4GB)...")
    matched = 0
    total = 0
    limit = 500 if test_mode else None

    for t in stream_dump("page", "page", max_insert_lines=limit):
        total += 1
        # Columns:
        #   0: page_id, 1: page_namespace, 2: page_title,
        #   3: page_is_redirect, 4: page_is_new, 5: page_random,
        #   6: page_touched, 7: page_links_updated, 8: page_latest,
        #   9: page_len, 10: page_content_model, 11: page_lang
        if len(t) < 10:
            continue

        page_id = t[0]
        if page_id not in geo_pages:
            continue

        namespace = t[1]
        is_redirect = t[3]

        # Only main namespace articles, no redirects
        if namespace != 0 or is_redirect == 1:
            del geo_pages[page_id]
            continue

        title = t[2]
        page_len = t[9]

        geo_pages[page_id]["title"] = title
        geo_pages[page_id]["page_len"] = int(page_len) if page_len else 0
        matched += 1

        if total % 500_000 == 0:
            print(f"  ...scanned {total:,} pages, matched {matched:,}")

    # Remove pages that weren't found in the page table
    to_remove = [pid for pid, info in geo_pages.items() if "title" not in info]
    for pid in to_remove:
        del geo_pages[pid]

    print(f"  Scanned {total:,} pages → {len(geo_pages):,} articles matched")


def step3_page_props(geo_pages, test_mode):
    """Get lead images, Wikidata QIDs, and short descriptions."""
    print(f"\n→ Step 3/5: Streaming page_props (~1GB)...")
    total = 0
    target_props = {'page_image_free', 'wikibase_item', 'wikibase-shortdesc'}
    limit = 500 if test_mode else None

    for t in stream_dump("page_props", "page_props", max_insert_lines=limit):
        total += 1
        # Columns: pp_page, pp_propname, pp_value, pp_sortkey
        if len(t) < 3:
            continue

        page_id = t[0]
        propname = t[1]

        if page_id not in geo_pages or propname not in target_props:
            continue

        value = t[2] or ""
        if propname == 'page_image_free' and value:
            geo_pages[page_id]["image"] = value
        elif propname == 'wikibase_item' and value:
            geo_pages[page_id]["qid"] = value
        elif propname == 'wikibase-shortdesc' and value:
            geo_pages[page_id]["description"] = value

    images = sum(1 for p in geo_pages.values() if "image" in p)
    qids = sum(1 for p in geo_pages.values() if "qid" in p)
    descs = sum(1 for p in geo_pages.values() if "description" in p)
    print(f"  Found {images:,} images, {qids:,} QIDs, {descs:,} descriptions")


def step4_linktarget(geo_pages, test_mode):
    """Map page titles to linktarget IDs (needed for pagelinks lookup)."""
    print(f"\n→ Step 4/5: Streaming linktarget (~1.4GB)...")

    # Build title → page_id lookup
    title_to_pid = {}
    for pid, info in geo_pages.items():
        title = info.get("title", "")
        if title:
            title_to_pid[title] = pid

    lt_id_to_pid = {}
    total = 0
    limit = 500 if test_mode else None

    for t in stream_dump("linktarget", "linktarget", max_insert_lines=limit):
        total += 1
        # Columns: lt_id, lt_namespace, lt_title
        if len(t) < 3:
            continue

        lt_ns = t[1]
        if lt_ns != 0:
            continue

        lt_title = t[2]
        if lt_title in title_to_pid:
            lt_id_to_pid[t[0]] = title_to_pid[lt_title]

        if total % 1_000_000 == 0:
            print(f"  ...scanned {total:,} targets, mapped {len(lt_id_to_pid):,}")

    print(f"  Scanned {total:,} linktargets → mapped {len(lt_id_to_pid):,} to geo pages")
    return lt_id_to_pid


def step5_pagelinks(geo_pages, lt_id_to_pid, test_mode):
    """Count inbound article links for each geo page."""
    print(f"\n→ Step 5/5: Streaming pagelinks (~6.9GB)...")

    # Fast regex for integer-only pagelinks tuples — runs in C, not Python.
    # Matches (pl_from, 0, pl_target_id) — namespace 0 only.
    _NS0_RE = re.compile(rb'\(\d+,0,(\d+)\)')

    inlink_counts = {}
    lines = 0
    hits = 0
    filename = DUMP_FILES["pagelinks"]
    local_path = os.path.join(DUMP_LOCAL_DIR, filename)

    if os.path.exists(local_path):
        print(f"  Reading local: {local_path}")
        gz = gzip.open(local_path, 'rb')
    else:
        url = f"{DUMP_BASE_URL}/{filename}"
        print(f"  Streaming: {url}")
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        resp = urllib.request.urlopen(req)
        gz = gzip.GzipFile(fileobj=resp)

    insert_lines = 0
    limit = 500 if test_mode else None

    for line_bytes in gz:
        if not line_bytes.startswith(b'INSERT'):
            continue

        insert_lines += 1
        if limit and insert_lines > limit:
            break

        lines += 1
        for m in _NS0_RE.finditer(line_bytes):
            target_id = int(m.group(1))
            if target_id in lt_id_to_pid:
                pid = lt_id_to_pid[target_id]
                inlink_counts[pid] = inlink_counts.get(pid, 0) + 1
                hits += 1

        if lines % 500 == 0:
            print(f"  ...{lines:,} INSERT lines, {hits:,} hits")

    gz.close()

    # Store counts
    for pid, count in inlink_counts.items():
        geo_pages[pid]["inlink_count"] = count

    with_inlinks = sum(1 for p in geo_pages.values() if p.get("inlink_count", 0) > 0)
    print(f"  Scanned {lines:,} INSERT lines, {hits:,} hits → {with_inlinks:,} pages have inlinks")


# ── Elevation lookup ──────────────────────────────────────────

def _cop_dem_tile_key(lat, lon):
    """Return the Copernicus DEM 30m tile key for a lat/lon coordinate.

    Tiles are 1°×1°. The tile key encodes the SW corner.
    """
    import math
    tile_lat = math.floor(lat)
    tile_lon = math.floor(lon)

    ns = "N" if tile_lat >= 0 else "S"
    ew = "E" if tile_lon >= 0 else "W"
    lat_str = f"{ns}{abs(tile_lat):02d}_00"
    lon_str = f"{ew}{abs(tile_lon):03d}_00"
    return (tile_lat, tile_lon), f"Copernicus_DSM_COG_10_{lat_str}_{lon_str}_DEM"


def sample_elevations(rows):
    """Sample elevation from Copernicus DEM 30m COGs on S3 (no download).

    Uses threaded tile fetches for ~15-20x speedup over sequential access.
    """
    import rasterio
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Group row indices by tile
    tile_groups = defaultdict(list)
    for i, r in enumerate(rows):
        key, tile_name = _cop_dem_tile_key(r["latitude"], r["longitude"])
        tile_groups[(key, tile_name)].append(i)

    print(f"  {len(rows):,} points across {len(tile_groups):,} tiles")

    # Configure GDAL for efficient COG access
    gdal_env = rasterio.Env(
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
        GDAL_HTTP_MULTIPLEX="YES",
        GDAL_HTTP_MAX_RETRY="3",
        GDAL_HTTP_RETRY_DELAY="2",
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
    )

    def _fetch_tile(tile_name, indices):
        """Fetch elevation for all points in a single tile."""
        url = f"{COP_DEM_BASE}/{tile_name}/{tile_name}.tif"
        results = {}
        try:
            with rasterio.open(url) as src:
                coords = [
                    (rows[i]["longitude"], rows[i]["latitude"])
                    for i in indices
                ]
                for j, val in enumerate(src.sample(coords)):
                    results[indices[j]] = int(val[0])
        except Exception:
            for i in indices:
                results[i] = 0
        return results

    sampled = 0
    failed_tiles = 0
    done_tiles = 0

    with gdal_env:
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = {
                pool.submit(_fetch_tile, tile_name, indices): tile_name
                for (key, tile_name), indices in tile_groups.items()
            }
            for future in as_completed(futures):
                results = future.result()
                for idx, elev in results.items():
                    rows[idx]["elevation"] = elev
                    if elev != 0:
                        sampled += 1

                done_tiles += 1
                # Check if this tile had all zeros (missing/ocean)
                if all(v == 0 for v in results.values()):
                    failed_tiles += 1

                if done_tiles % 500 == 0:
                    print(f"  ...{done_tiles:,}/{len(tile_groups):,} tiles, "
                          f"{sampled:,} points sampled")

    print(f"  Sampled {len(rows) - failed_tiles:,} points from "
          f"{done_tiles - failed_tiles:,} tiles "
          f"({failed_tiles:,} missing/ocean tiles → elevation 0)")


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract geotagged Wikipedia articles into GeoParquet"
    )
    parser.add_argument("--test", action="store_true",
                        help="Test mode: process small subset of each dump")
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)

    print("=" * 60)
    print("wiki-geoparquet — Extract from Wikipedia SQL Dumps")
    if args.test:
        print("** TEST MODE — small subset only **")
    print("=" * 60)

    start = time.time()

    # Process dumps sequentially (each builds on previous state)
    geo_pages = step1_geo_tags(args.test)
    if not geo_pages:
        print("ERROR: No geotagged pages found!")
        sys.exit(1)

    step2_page(geo_pages, args.test)
    step3_page_props(geo_pages, args.test)
    lt_id_to_pid = step4_linktarget(geo_pages, args.test)
    step5_pagelinks(geo_pages, lt_id_to_pid, args.test)

    # ── Assemble rows ───────────────────────────────────────
    print(f"\n→ Assembling output...")
    rows = []
    for pid, info in geo_pages.items():
        title = info.get("title", "")
        if not title:
            continue

        image_file = info.get("image", "")
        image_url = ""
        if image_file:
            image_url = (
                "https://commons.wikimedia.org/wiki/Special:FilePath/"
                + image_file.replace(" ", "_")
            )

        rows.append({
            "page_id": pid,
            "qid": info.get("qid", ""),
            "label": title.replace("_", " "),
            "description": info.get("description", ""),
            "latitude": info["lat"],
            "longitude": info["lon"],
            "gt_type": info.get("gt_type", ""),
            "page_len": info.get("page_len", 0),
            "inlink_count": info.get("inlink_count", 0),
            "gt_primary": info.get("gt_primary", True),
            "wikipedia_url": "https://en.wikipedia.org/wiki/" + title,
            "image_url": image_url,
        })

    # ── Sample elevation ──────────────────────────────────
    print(f"\n→ Sampling elevation from Copernicus DEM 30m...")
    sample_elevations(rows)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"Complete in {elapsed / 60:.1f} minutes")
    print(f"  Total pages: {len(rows):,}")

    if not rows:
        print("ERROR: No pages extracted!")
        sys.exit(1)

    # Stats
    with_image = sum(1 for r in rows if r["image_url"])
    with_qid = sum(1 for r in rows if r["qid"])
    with_inlinks = sum(1 for r in rows if r["inlink_count"] > 0)
    avg_len = sum(r["page_len"] for r in rows) / len(rows)
    avg_inlinks = sum(r["inlink_count"] for r in rows) / len(rows)

    print(f"  With images:  {with_image:,} ({100 * with_image / len(rows):.0f}%)")
    print(f"  With QIDs:    {with_qid:,}")
    print(f"  With inlinks: {with_inlinks:,}")
    print(f"  Avg page_len: {avg_len:,.0f} bytes")
    print(f"  Avg inlinks:  {avg_inlinks:,.0f}")

    # ── DuckDB: add geometry, Hilbert-sort, write GeoParquet ──
    print(f"\n→ Writing GeoParquet (Hilbert-sorted)...")
    import duckdb
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    import pyarrow as pa

    table = pa.table({
        "page_id": pa.array([r["page_id"] for r in rows], type=pa.int32()),
        "qid": pa.array([r["qid"] for r in rows], type=pa.string()),
        "label": pa.array([r["label"] for r in rows], type=pa.string()),
        "description": pa.array([r["description"] for r in rows], type=pa.string()),
        "latitude": pa.array([r["latitude"] for r in rows], type=pa.float64()),
        "longitude": pa.array([r["longitude"] for r in rows], type=pa.float64()),
        "elevation": pa.array([r["elevation"] for r in rows], type=pa.int16()),
        "gt_type": pa.array([r["gt_type"] for r in rows], type=pa.string()),
        "page_len": pa.array([r["page_len"] for r in rows], type=pa.int32()),
        "inlink_count": pa.array([r["inlink_count"] for r in rows], type=pa.int32()),
        "gt_primary": pa.array([r["gt_primary"] for r in rows], type=pa.bool_()),
        "wikipedia_url": pa.array([r["wikipedia_url"] for r in rows], type=pa.string()),
        "image_url": pa.array([r["image_url"] for r in rows], type=pa.string()),
    })
    del rows  # free memory

    db.register("raw", table)

    row_group_size = min(75_000, max(5_000, len(geo_pages) // 10))

    db.execute(f"""
        COPY (
            SELECT
                ST_Point(longitude, latitude) AS geometry,
                page_id, qid, label, description, elevation, gt_type,
                page_len, inlink_count, gt_primary, wikipedia_url, image_url
            FROM raw
            ORDER BY ST_Hilbert(ST_Point(longitude, latitude))
        ) TO '{OUTPUT_FILE}'
        WITH (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            ROW_GROUP_SIZE {row_group_size}
        )
    """)

    db.close()

    # Add bbox covering metadata for spatial predicate pushdown
    print(f"\n→ Adding bbox covering metadata...")
    import subprocess
    subprocess.run(
        ["uv", "run", "gpio", "add", "bbox", OUTPUT_FILE, OUTPUT_FILE],
        check=True,
    )

    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nWritten to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")
    print("Done!")


if __name__ == "__main__":
    main()
