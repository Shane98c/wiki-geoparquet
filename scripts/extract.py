"""
Extract geotagged Wikipedia articles using Wikidata coordinates + Wikipedia dumps.

Coordinates and enrichment come from the Wikidata truthy N-Triples dump (~70GB):
  - P625:  coordinate location
  - P31:   instance of (type classification)
  - P17:   country
  - P1082: population
  - P1566: GeoNames ID
  - P18:   image (fallback for page_image_free)
  - P6802: related images

Article metadata comes from Wikipedia SQL dumps (~12GB):
  - page_props: QID→page_id mapping, lead images, descriptions
  - geo_tags:   supplementary gt_type classification
  - page:       titles, page lengths; filters redirects
  - linktarget: link target ID mappings
  - pagelinks:  inbound link counts

Outputs a Hilbert-sorted GeoParquet 1.1 file with bbox covering metadata.
Use --test to validate with a small subset first.
"""

import argparse
import gzip
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

sys.stdout.reconfigure(line_buffering=True)

WIKIDATA_NT_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-truthy.nt.gz"
DUMP_BASE_URL = "https://dumps.wikimedia.org/enwiki/latest"
DUMP_LOCAL_DIR = "data/dumps"
OUTPUT_FILE = "data/wikipedia_geotagged.parquet"
QID_LABEL_BATCH_SIZE = 50
QID_LABEL_MAX_ATTEMPTS = 8
QID_LABEL_REQUEST_DELAY = 1.0
USER_AGENT = "wiki-geoparquet/1.0 (github.com/Shane98c/wiki-geoparquet)"

COP_DEM_BASE = "https://copernicus-dem-30m.s3.amazonaws.com"

DUMP_FILES = {
    "geo_tags":   "enwiki-latest-geo_tags.sql.gz",
    "page":       "enwiki-latest-page.sql.gz",
    "page_props": "enwiki-latest-page_props.sql.gz",
    "linktarget": "enwiki-latest-linktarget.sql.gz",
    "pagelinks":  "enwiki-latest-pagelinks.sql.gz",
}

# Properties to extract from the Wikidata truthy N-Triples dump
WIKIDATA_PROPERTIES = ["P625", "P31", "P17", "P1082", "P1566", "P18", "P6802"]

# Regex to parse NT triples: <entity/QID> <prop/direct/PID> object .
_NT_RE = re.compile(
    rb'<http://www\.wikidata\.org/entity/(Q\d+)> '
    rb'<http://www\.wikidata\.org/prop/direct/(P\d+)> '
    rb'(.+?) \.\s*$'
)


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


# ── Wikidata N-Triples processing ────────────────────────────

def _parse_nt_value(raw_bytes, prop):
    """Parse an N-Triples object value based on the property type."""
    raw = raw_bytes.decode("utf-8", errors="replace")

    if prop == "P625":
        # Format: "Point(lon lat)"^^geo:wktLiteral — default globe is Earth.
        # Non-Earth coords have a globe URI prefix:
        #   "<http://www.wikidata.org/entity/Q405> Point(...)"^^geo:wktLiteral
        # Only Q2 (Earth) is acceptable; reject Moon/Mars/etc. so they don't
        # get plotted on Earth at bogus locations.
        m = re.search(r'"(?:<([^>]+)>\s+)?Point\(([^ ]+) ([^)]+)\)"', raw)
        if not m:
            return None
        globe = m.group(1)
        if globe and not globe.endswith("/entity/Q2"):
            return None
        lon, lat = float(m.group(2)), float(m.group(3))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return (lat, lon)
        return None

    if prop in ("P31", "P17"):
        m = re.search(r'entity/(Q\d+)', raw)
        return m.group(1) if m else None

    if prop == "P1082":
        m = re.search(r'"([+-]?\d+)', raw)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
        return None

    if prop == "P1566":
        m = re.search(r'"([^"]+)"', raw)
        return m.group(1) if m else None

    if prop in ("P18", "P6802"):
        m = re.search(r'<(http[^>]+)>', raw)
        if not m:
            return None
        # RDF dump serves http:// Commons URLs; force https to avoid
        # mixed-content blocking when loaded from HTTPS pages.
        return m.group(1).replace("http://", "https://", 1)

    return None


def step1_wikidata(test_mode):
    """Extract P625 coordinates + enrichment from Wikidata truthy N-Triples."""
    print("\n→ Step 1/6: Streaming Wikidata truthy N-Triples dump...")

    grep_pattern = "|".join(f"/direct/{p}>" for p in WIKIDATA_PROPERTIES)

    # curl uses --fail so HTTP errors exit non-zero; pipefail propagates any
    # stage's failure so a truncated download surfaces as a non-zero exit code
    # instead of silently succeeding via grep's 0 status on partial input.
    if test_mode:
        cmd = (
            f'curl -sSL --fail --retry 3 --retry-delay 5 '
            f'-H "User-Agent: {USER_AGENT}" "{WIKIDATA_NT_URL}" '
            f'| gunzip '
            f'| head -5000000 '
            f'| grep -E "{grep_pattern}"'
        )
        print(f"  ** TEST MODE — first 5M lines only **")
    else:
        cmd = (
            f'curl -sSL --fail --retry 3 --retry-delay 5 '
            f'-H "User-Agent: {USER_AGENT}" "{WIKIDATA_NT_URL}" '
            f'| gunzip '
            f'| grep -E "{grep_pattern}"'
        )

    print(f"  URL: {WIKIDATA_NT_URL}")
    print(f"  Properties: {', '.join(WIKIDATA_PROPERTIES)}")

    # Triples in the NT dump are grouped by entity, so we buffer the current
    # entity's properties and only commit to `wikidata` at the entity boundary
    # if P625 was seen. Without this, ~32M entities (P31/P17/etc. but no P625)
    # would accumulate in memory and be discarded at the end — causing GC
    # pressure that degrades throughput by 3x on GH Actions runners.
    wikidata = {}
    buffer = {}
    current_qid = None
    total = 0
    start = time.time()

    # Invoke via bash -o pipefail so a failure in any pipeline stage (curl
    # disconnect, gunzip error) surfaces as a non-zero exit, instead of grep
    # reporting success on a truncated stream.
    proc = subprocess.Popen(
        ["bash", "-o", "pipefail", "-c", cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for line in proc.stdout:
        m = _NT_RE.match(line)
        if not m:
            continue

        qid = m.group(1).decode()
        prop = m.group(2).decode()
        value = _parse_nt_value(m.group(3), prop)

        if value is None:
            continue

        total += 1

        # Entity transition: commit the previous entity if it had P625.
        if qid != current_qid:
            if current_qid is not None and "lat" in buffer:
                wikidata[current_qid] = buffer
            buffer = {}
            current_qid = qid

        if prop == "P625":
            buffer["lat"], buffer["lon"] = value
        elif prop == "P31" and "instance_of" not in buffer:
            buffer["instance_of"] = value
            buffer["instance_of_qid"] = value
        elif prop == "P17" and "country" not in buffer:
            buffer["country"] = value
        elif prop == "P1082":
            buffer["population"] = value
        elif prop == "P1566":
            buffer["geonames_id"] = value
        elif prop == "P18":
            buffer["p18_image"] = value
        elif prop == "P6802":
            buffer.setdefault("related_images", []).append(value)

        if total % 500_000 == 0:
            print(f"  ...{total:,} triples, {len(wikidata):,} items with P625")

    # Commit final buffered entity
    if current_qid is not None and "lat" in buffer:
        wikidata[current_qid] = buffer

    returncode = proc.wait()
    # In test mode the `head` stage deliberately closes the pipe early, which
    # propagates SIGPIPE up to curl and yields a non-zero exit — expected, not
    # a real failure. In production any non-zero exit means a truncated stream.
    if returncode != 0 and not test_mode:
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Wikidata NT dump pipeline exited with code {returncode} — "
            f"refusing to publish a truncated dataset. stderr: {stderr.strip()}"
        )

    elapsed = time.time() - start
    print(f"  Processed {total:,} triples in {elapsed / 60:.1f} minutes")
    print(f"  {len(wikidata):,} items with P625 coordinates")

    return wikidata


def _resolve_qid_labels(entries):
    """Resolve P31 and P17 QIDs to human-readable labels via Wikidata API.

    Called after page_props filtering so we only resolve QIDs for items that
    actually survive into the output — avoids unnecessary API calls and
    avoids failing the build on missing labels for QIDs that would have been
    filtered out anyway.
    """
    print("\n→ Resolving instance_of/country labels from Wikidata API...")
    start = time.time()

    qids_to_resolve = set()
    for v in entries.values():
        if v.get("instance_of"):
            qids_to_resolve.add(v["instance_of"])
        if v.get("country"):
            qids_to_resolve.add(v["country"])

    if not qids_to_resolve:
        print("  No QIDs to resolve")
        return

    print(f"  {len(qids_to_resolve):,} unique QIDs to resolve")

    labels = {}
    qid_list = sorted(qids_to_resolve)

    for i in range(0, len(qid_list), QID_LABEL_BATCH_SIZE):
        if i > 0:
            # Pace requests — Wikimedia throttles bursts, and GitHub-runner
            # IPs are rate-limited far harder than residential ones.
            time.sleep(QID_LABEL_REQUEST_DELAY)
        batch = qid_list[i:i + QID_LABEL_BATCH_SIZE]
        batch_num = i // QID_LABEL_BATCH_SIZE + 1
        ids = "|".join(batch)
        url = (
            "https://www.wikidata.org/w/api.php?action=wbgetentities"
            f"&ids={ids}&props=labels&languages=en&format=json"
        )
        # Retry with backoff. On 429/503 honor Retry-After — the throttle
        # window outlasts short sleeps, so wait generously before retrying.
        last_err = None
        for attempt in range(QID_LABEL_MAX_ATTEMPTS):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                resp = urllib.request.urlopen(req, timeout=30)
                data = json.loads(resp.read())
                if "error" in data:
                    raise RuntimeError(data["error"])
                for qid, entity in data.get("entities", {}).items():
                    label = entity.get("labels", {}).get("en", {}).get("value")
                    if label:
                        labels[qid] = label
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt == QID_LABEL_MAX_ATTEMPTS - 1:
                    break
                if getattr(e, "code", None) in (429, 503):
                    header = e.headers.get("Retry-After") if e.headers else None
                    wait = (
                        int(header)
                        if header and header.isdigit()
                        else min(5 * 2 ** attempt, 120)
                    )
                    print(
                        f"  Wikidata returned HTTP {e.code}; waiting "
                        f"{wait}s before retry "
                        f"{attempt + 2}/{QID_LABEL_MAX_ATTEMPTS}"
                    )
                else:
                    wait = 2 ** attempt
                time.sleep(wait)
        if last_err is not None:
            raise RuntimeError(
                f"Wikidata label resolution failed for batch "
                f"{batch_num} after {QID_LABEL_MAX_ATTEMPTS} attempts "
                f"— refusing to publish data with raw Q-IDs. "
                f"Last error: {last_err}"
            ) from last_err

        if batch_num % 20 == 0:
            print(f"  ...resolved {len(labels):,}/{len(qids_to_resolve):,}")

    elapsed = time.time() - start
    print(f"  Resolved {len(labels):,} labels ({elapsed / 60:.1f}m)")

    unresolved = qids_to_resolve - set(labels)
    if unresolved:
        print(f"  {len(unresolved):,} QIDs have no English label; dropping those values")

    for v in entries.values():
        qid = v.get("instance_of", "")
        if qid:
            v["instance_of"] = labels.get(qid, "")
        qid = v.get("country", "")
        if qid:
            v["country"] = labels.get(qid, "")


# ── Processing steps ──────────────────────────────────────────

def step2_page_props(wikidata, test_mode):
    """Map QID→page_id via page_props; collect images and descriptions."""
    print(f"\n→ Step 2/6: Streaming page_props (~1GB)...")
    start = time.time()
    total = 0
    target_props = {'page_image_free', 'wikibase_item', 'wikibase-shortdesc'}
    limit = 500 if test_mode else None

    page_qids = {}
    page_images = {}
    page_descs = {}

    for t in stream_dump("page_props", "page_props", max_insert_lines=limit):
        total += 1
        if len(t) < 3:
            continue

        page_id = t[0]
        propname = t[1]

        if propname not in target_props:
            continue

        value = t[2] or ""
        if propname == 'wikibase_item' and value:
            page_qids[page_id] = value
        elif propname == 'page_image_free' and value:
            page_images[page_id] = value
        elif propname == 'wikibase-shortdesc' and value:
            page_descs[page_id] = value

    # Build geo_pages for pages whose QID has P625 in Wikidata
    geo_pages = {}
    for page_id, qid in page_qids.items():
        if qid not in wikidata:
            continue

        wd = wikidata[qid]
        image_file = page_images.get(page_id, "")

        # page_image_free as primary image, P18 as fallback
        if image_file:
            image_url = (
                "https://commons.wikimedia.org/wiki/Special:FilePath/"
                + image_file.replace(" ", "_")
            )
        elif wd.get("p18_image"):
            image_url = wd["p18_image"]
        else:
            image_url = ""

        geo_pages[page_id] = {
            "lat": wd["lat"],
            "lon": wd["lon"],
            "qid": qid,
            "instance_of": wd.get("instance_of", ""),
            "instance_of_qid": wd.get("instance_of_qid", ""),
            "country": wd.get("country", ""),
            "population": wd.get("population"),
            "geonames_id": wd.get("geonames_id", ""),
            "image_url": image_url,
            "related_images": wd.get("related_images", []),
            "description": page_descs.get(page_id, ""),
        }

    with_image = sum(1 for p in geo_pages.values() if p["image_url"])
    with_desc = sum(1 for p in geo_pages.values() if p["description"])
    elapsed = time.time() - start
    print(f"  Scanned {total:,} page_props → {len(geo_pages):,} pages with Wikidata P625 ({elapsed / 60:.1f}m)")
    print(f"  With images: {with_image:,}, with descriptions: {with_desc:,}")

    return geo_pages


def step3_geo_tags(geo_pages, test_mode):
    """Get supplementary gt_type from geo_tags.

    Pages can have multiple geo_tags entries; prefer the primary Earth coord
    so that multi-coord articles get the type describing the article's main
    location rather than an incidental secondary tag.
    """
    print(f"\n→ Step 3/6: Streaming geo_tags for gt_type (~52MB)...")
    start = time.time()
    total = 0
    matched = 0
    limit = 50 if test_mode else None

    for t in stream_dump("geo_tags", "geo_tags", max_insert_lines=limit):
        total += 1
        # Columns: 0:gt_id, 1:gt_page_id, 2:gt_globe, 3:gt_primary,
        # 4:gt_lat, 5:gt_lon, 6:gt_dim, 7:gt_type
        if len(t) < 8:
            continue

        page_id = t[1]
        if page_id not in geo_pages:
            continue

        if t[2] != 'earth':
            continue

        gt_type = t[7] or ""
        if not gt_type:
            continue

        is_primary = t[3] == 1
        existing = geo_pages[page_id].get("_gt_type_primary")

        # Take first value; upgrade to primary if we later see one.
        if existing is None:
            geo_pages[page_id]["gt_type"] = gt_type
            geo_pages[page_id]["_gt_type_primary"] = is_primary
            matched += 1
        elif is_primary and not existing:
            geo_pages[page_id]["gt_type"] = gt_type
            geo_pages[page_id]["_gt_type_primary"] = True

    # Drop the internal tracking key
    for info in geo_pages.values():
        info.pop("_gt_type_primary", None)

    elapsed = time.time() - start
    print(f"  Scanned {total:,} geo_tags → {matched:,} pages got gt_type ({elapsed / 60:.1f}m)")


def step4_page(geo_pages, test_mode):
    """Get page titles, lengths; filter out redirects and non-articles."""
    print(f"\n→ Step 4/6: Streaming page (~2.4GB)...")
    start = time.time()
    matched = 0
    total = 0
    limit = 500 if test_mode else None

    for t in stream_dump("page", "page", max_insert_lines=limit):
        total += 1
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

    elapsed = time.time() - start
    print(f"  Scanned {total:,} pages → {len(geo_pages):,} articles matched ({elapsed / 60:.1f}m)")


# Match `(id,0,'title')` linktarget rows where ns=0. The title capture allows
# backslash-escape sequences since MediaWiki SQL dumps escape special chars.
# Skipping non-ns=0 tuples this way avoids parsing ~90% of the dump.
_LT_NS0_RE = re.compile(rb"\((\d+),0,'((?:[^'\\]|\\.)*)'\)")


def step5_linktarget(geo_pages, test_mode):
    """Map page titles to linktarget IDs (needed for pagelinks lookup)."""
    print(f"\n→ Step 5/6: Streaming linktarget (~1.4GB)...")
    start = time.time()

    # Re-encode titles as their raw SQL-dump byte form so we can match
    # against linktarget bytes directly, skipping per-row UTF-8 decode.
    # Order matters: escape backslash first, then the quote chars it could
    # produce. MediaWiki titles don't contain control chars in practice.
    title_bytes_to_pid = {}
    for pid, info in geo_pages.items():
        title = info.get("title", "")
        if title:
            escaped = (title.replace("\\", "\\\\")
                            .replace("'", "\\'")
                            .replace('"', '\\"'))
            title_bytes_to_pid[escaped.encode("utf-8")] = pid

    filename = DUMP_FILES["linktarget"]
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

    lt_id_to_pid = {}
    insert_lines = 0
    matched_ns0 = 0
    limit = 500 if test_mode else None

    for line in gz:
        if not line.startswith(b'INSERT'):
            continue
        insert_lines += 1
        if limit and insert_lines > limit:
            break

        for m in _LT_NS0_RE.finditer(line):
            matched_ns0 += 1
            pid = title_bytes_to_pid.get(m.group(2))
            if pid is not None:
                lt_id_to_pid[int(m.group(1))] = pid

        if insert_lines % 500 == 0:
            print(f"  ...{insert_lines:,} INSERT lines, "
                  f"{matched_ns0:,} ns=0 rows, {len(lt_id_to_pid):,} mapped")

    gz.close()

    elapsed = time.time() - start
    print(f"  Scanned {insert_lines:,} INSERT lines, {matched_ns0:,} ns=0 rows "
          f"→ mapped {len(lt_id_to_pid):,} to geo pages ({elapsed / 60:.1f}m)")
    return lt_id_to_pid


def step6_pagelinks(geo_pages, lt_id_to_pid, test_mode):
    """Count inbound article links for each geo page."""
    print(f"\n→ Step 6/6: Streaming pagelinks (~6.9GB)...")
    start = time.time()

    _NS0_RE = re.compile(rb'\(\d+,0,(\d+)\)')
    # Regex groups are bytes already; keep the hot-path lookup in bytes form
    # so we avoid int() conversion for every namespace-0 pagelink candidate.
    target_id_bytes_to_pid = {
        str(target_id).encode("ascii"): pid
        for target_id, pid in lt_id_to_pid.items()
    }

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
            pid = target_id_bytes_to_pid.get(m.group(1))
            if pid is not None:
                inlink_counts[pid] = inlink_counts.get(pid, 0) + 1
                hits += 1

        if lines % 500 == 0:
            print(f"  ...{lines:,} INSERT lines, {hits:,} hits")

    gz.close()

    for pid, count in inlink_counts.items():
        geo_pages[pid]["inlink_count"] = count

    with_inlinks = sum(1 for p in geo_pages.values() if p.get("inlink_count", 0) > 0)
    elapsed = time.time() - start
    print(f"  Scanned {lines:,} INSERT lines, {hits:,} hits → "
          f"{with_inlinks:,} pages have inlinks ({elapsed / 60:.1f}m)")


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

    start = time.time()
    tile_groups = defaultdict(list)
    for i, r in enumerate(rows):
        key, tile_name = _cop_dem_tile_key(r["latitude"], r["longitude"])
        tile_groups[(key, tile_name)].append(i)

    print(f"  {len(rows):,} points across {len(tile_groups):,} tiles")

    gdal_env = rasterio.Env(
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
        GDAL_HTTP_MULTIPLEX="YES",
        GDAL_HTTP_MAX_RETRY="3",
        GDAL_HTTP_RETRY_DELAY="2",
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif",
    )

    def _fetch_tile(tile_name, indices):
        url = f"{COP_DEM_BASE}/{tile_name}/{tile_name}.tif"
        results = {}
        try:
            with rasterio.open(url) as src:
                def _nudge(v):
                    return v + 0.001 if v == int(v) else v
                coords = [
                    (_nudge(rows[i]["longitude"]), _nudge(rows[i]["latitude"]))
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
                if all(v == 0 for v in results.values()):
                    failed_tiles += 1

                if done_tiles % 500 == 0:
                    print(f"  ...{done_tiles:,}/{len(tile_groups):,} tiles, "
                          f"{sampled:,} points sampled")

    elapsed = time.time() - start
    print(f"  Sampled {len(rows) - failed_tiles:,} points from "
          f"{done_tiles - failed_tiles:,} tiles "
          f"({failed_tiles:,} missing/ocean tiles → elevation 0) ({elapsed / 60:.1f}m)")


# ── Main ──────────────────────────────────────────────────────

def _save_wikidata_cache(wikidata, path):
    """Save the wikidata dict to parquet for cross-job caching."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    qids = sorted(wikidata.keys())
    table = pa.table({
        "qid": pa.array(qids, type=pa.string()),
        "lat": pa.array([wikidata[q]["lat"] for q in qids], type=pa.float64()),
        "lon": pa.array([wikidata[q]["lon"] for q in qids], type=pa.float64()),
        "instance_of": pa.array([wikidata[q].get("instance_of", "") for q in qids], type=pa.string()),
        "instance_of_qid": pa.array([wikidata[q].get("instance_of_qid", "") for q in qids], type=pa.string()),
        "country": pa.array([wikidata[q].get("country", "") for q in qids], type=pa.string()),
        "population": pa.array([wikidata[q].get("population") for q in qids], type=pa.int64()),
        "geonames_id": pa.array([wikidata[q].get("geonames_id", "") for q in qids], type=pa.string()),
        "p18_image": pa.array([wikidata[q].get("p18_image", "") for q in qids], type=pa.string()),
        "related_images": pa.array([wikidata[q].get("related_images", []) for q in qids],
                                   type=pa.list_(pa.string())),
    })
    pq.write_table(table, path, compression="zstd")
    del table
    print(f"  Saved {len(qids):,} items to {path} ({os.path.getsize(path) / 1048576:.0f} MiB)")


def _load_wikidata_cache(path):
    """Load the wikidata dict from a cached parquet file."""
    import pyarrow.parquet as pq

    print(f"\n→ Loading cached Wikidata from {path}...")
    table = pq.read_table(path)
    # Older caches (pre-instance_of_qid column) stored the QID in instance_of
    # since caching happens before label resolution — fall back to that.
    has_qid_col = "instance_of_qid" in table.column_names
    wikidata = {}
    for i in range(len(table)):
        qid = table["qid"][i].as_py()
        entry = {"lat": table["lat"][i].as_py(), "lon": table["lon"][i].as_py()}
        for col in ("instance_of", "country", "geonames_id", "p18_image"):
            v = table[col][i].as_py()
            if v:
                entry[col] = v
        iq = table["instance_of_qid"][i].as_py() if has_qid_col else entry.get("instance_of", "")
        if iq:
            entry["instance_of_qid"] = iq
        pop = table["population"][i].as_py()
        if pop is not None:
            entry["population"] = pop
        imgs = table["related_images"][i].as_py()
        if imgs:
            entry["related_images"] = imgs
        wikidata[qid] = entry
    del table
    print(f"  Loaded {len(wikidata):,} items with P625 coordinates")
    return wikidata


def main():
    parser = argparse.ArgumentParser(
        description="Extract geotagged Wikipedia articles into GeoParquet"
    )
    parser.add_argument("--test", action="store_true",
                        help="Test mode: process small subset of each dump")
    parser.add_argument("--save-wikidata",
                        help="Run step 1 only, save result to parquet, then exit")
    parser.add_argument("--load-wikidata",
                        help="Skip step 1, load wikidata from cached parquet")
    args = parser.parse_args()

    os.makedirs("data", exist_ok=True)

    print("=" * 60)
    print("wiki-geoparquet — Extract from Wikidata + Wikipedia Dumps")
    if args.test:
        print("** TEST MODE — small subset only **")
    print("=" * 60)

    start = time.time()

    # Step 1: Wikidata coordinates + enrichment
    if args.load_wikidata:
        wikidata = _load_wikidata_cache(args.load_wikidata)
    else:
        wikidata = step1_wikidata(args.test)
    if not wikidata:
        print("ERROR: No Wikidata coordinates found!")
        sys.exit(1)

    if args.save_wikidata:
        _save_wikidata_cache(wikidata, args.save_wikidata)
        elapsed = time.time() - start
        print(f"\nStep 1 complete in {elapsed / 60:.1f} minutes — exiting.")
        sys.exit(0)

    # Step 2: Map QIDs to Wikipedia page_ids
    geo_pages = step2_page_props(wikidata, args.test)
    del wikidata

    if not geo_pages:
        print("ERROR: No pages matched Wikidata coordinates!")
        sys.exit(1)

    # Resolve QID labels only for items that survived the enwiki join —
    # avoids wasted API calls and false failures on QIDs that would be
    # filtered out anyway.
    _resolve_qid_labels(geo_pages)

    # Steps 3-6: Wikipedia dump enrichment
    step3_geo_tags(geo_pages, args.test)
    step4_page(geo_pages, args.test)
    lt_id_to_pid = step5_linktarget(geo_pages, args.test)
    step6_pagelinks(geo_pages, lt_id_to_pid, args.test)

    # ── Assemble rows ───────────────────────────────────────
    print(f"\n→ Assembling output...")
    rows = []
    for pid, info in geo_pages.items():
        title = info.get("title", "")
        if not title:
            continue

        rows.append({
            "page_id": pid,
            "qid": info.get("qid", ""),
            "label": title.replace("_", " "),
            "description": info.get("description", ""),
            "instance_of": info.get("instance_of", ""),
            "instance_of_qid": info.get("instance_of_qid", ""),
            "country": info.get("country", ""),
            "population": info.get("population"),
            "geonames_id": info.get("geonames_id", ""),
            "latitude": info["lat"],
            "longitude": info["lon"],
            "gt_type": info.get("gt_type", ""),
            "page_len": info.get("page_len", 0),
            "inlink_count": info.get("inlink_count", 0),
            "wikipedia_url": "https://en.wikipedia.org/wiki/" + title,
            "image_url": info.get("image_url", ""),
            "related_images": info.get("related_images", []),
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
    with_inlinks = sum(1 for r in rows if r["inlink_count"] > 0)
    with_instance = sum(1 for r in rows if r["instance_of"])
    with_instance_qid = sum(1 for r in rows if r["instance_of_qid"])
    with_country = sum(1 for r in rows if r["country"])
    with_pop = sum(1 for r in rows if r["population"] is not None)
    with_geonames = sum(1 for r in rows if r["geonames_id"])
    with_related = sum(1 for r in rows if r["related_images"])
    avg_len = sum(r["page_len"] for r in rows) / len(rows)
    avg_inlinks = sum(r["inlink_count"] for r in rows) / len(rows)

    print(f"  With images:      {with_image:,} ({100 * with_image / len(rows):.0f}%)")
    print(f"  With instance_of: {with_instance:,} ({100 * with_instance / len(rows):.0f}%)")
    print(f"  With instance_of_qid: {with_instance_qid:,} ({100 * with_instance_qid / len(rows):.0f}%)")
    print(f"  With country:     {with_country:,} ({100 * with_country / len(rows):.0f}%)")
    print(f"  With population:  {with_pop:,} ({100 * with_pop / len(rows):.0f}%)")
    print(f"  With GeoNames ID: {with_geonames:,} ({100 * with_geonames / len(rows):.0f}%)")
    print(f"  With related img: {with_related:,} ({100 * with_related / len(rows):.0f}%)")
    print(f"  With inlinks:     {with_inlinks:,}")
    print(f"  Avg page_len:     {avg_len:,.0f} bytes")
    print(f"  Avg inlinks:      {avg_inlinks:,.0f}")

    # ── DuckDB: write GeoParquet with geometry column ──
    print(f"\n→ Writing GeoParquet...")
    import duckdb
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    import pyarrow as pa

    table = pa.table({
        "page_id": pa.array([r["page_id"] for r in rows], type=pa.int32()),
        "qid": pa.array([r["qid"] for r in rows], type=pa.string()),
        "label": pa.array([r["label"] for r in rows], type=pa.string()),
        "description": pa.array([r["description"] for r in rows], type=pa.string()),
        "instance_of": pa.array([r["instance_of"] for r in rows], type=pa.string()),
        "instance_of_qid": pa.array([r["instance_of_qid"] for r in rows], type=pa.string()),
        "country": pa.array([r["country"] for r in rows], type=pa.string()),
        "population": pa.array([r["population"] for r in rows], type=pa.int64()),
        "geonames_id": pa.array([r["geonames_id"] for r in rows], type=pa.string()),
        "latitude": pa.array([r["latitude"] for r in rows], type=pa.float64()),
        "longitude": pa.array([r["longitude"] for r in rows], type=pa.float64()),
        "elevation": pa.array([r["elevation"] for r in rows], type=pa.int16()),
        "gt_type": pa.array([r["gt_type"] for r in rows], type=pa.string()),
        "page_len": pa.array([r["page_len"] for r in rows], type=pa.int32()),
        "inlink_count": pa.array([r["inlink_count"] for r in rows], type=pa.int32()),
        "wikipedia_url": pa.array([r["wikipedia_url"] for r in rows], type=pa.string()),
        "image_url": pa.array([r["image_url"] for r in rows], type=pa.string()),
        "related_images": pa.array([r["related_images"] for r in rows],
                                   type=pa.list_(pa.string())),
    })
    del rows  # free memory

    db.register("raw", table)

    db.execute(f"""
        COPY (
            SELECT
                ST_Point(longitude, latitude) AS geometry,
                page_id, qid, label, description, instance_of, instance_of_qid,
                country, population, geonames_id, elevation, gt_type,
                page_len, inlink_count, wikipedia_url, image_url,
                related_images
            FROM raw
        ) TO '{OUTPUT_FILE}'
        WITH (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    db.close()

    # Hilbert-sort + add bbox covering in one pass
    print(f"\n→ Hilbert-sorting with bbox covering...")
    subprocess.run(
        ["uv", "run", "gpio", "sort", "hilbert", "--add-bbox",
         "--row-group-size", "25000",
         OUTPUT_FILE, OUTPUT_FILE],
        check=True,
    )

    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nWritten to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")
    print("Done!")


if __name__ == "__main__":
    main()
