"""
Feature size from OpenStreetMap, as one bulk extract.

Every OSM way and relation tagged `wikidata=` or `wikipedia=` (nodes have no
size) is pulled from the QLever osm-planet SPARQL endpoint with its envelope
and, for areas, osm2rdf's area, in a few paged queries. The result is one
Parquet per month, data/osm/extent-YYYY-MM.parquet, with one row per OSM
object: type, id, QID, Wikipedia language and title (the dump's underscore
form), bbox, `extent_km` (the bbox's longer side) and `area_km2`. An
envelope that spans the antimeridian is flagged and carries no extent.

The build joins the newest extract it finds: by QID, by `en:` title, and by
the Wikidata P402 relation id; of several OSM objects for one article the
largest extent wins. No live query happens in the build.

    osm_extent.py --extract [--month YYYY-MM]   write this month's extract
    osm_extent.py --report --source PARQUET [--top 2000]
"""

import argparse
import csv
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

sys.stdout.reconfigure(line_buffering=True)

QLEVER_URL = "https://qlever.dev/api/osm-planet"
CACHE_DIR = "data/osm"
CACHE_URL_BASE = "https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/osm"
USER_AGENT = ("wiki-geoparquet/1.0 (github.com/Shane98c/wiki-geoparquet; "
              "shane98c@gmail.com)")
PAGE_ROWS = 250_000
MAX_ATTEMPTS = 4
# How far back the build looks for an extract before giving up on the column.
LOOKBACK_MONTHS = 12

# instance_of labels the coverage report counts as natural or protected-area
# classes, the features whose size matters.
CLASS_RE = (
    r"\b(lake|reservoir|lagoon|loch|lough|pond|wetland|swamp|marsh|bog|"
    r"island|islet|archipelago|atoll|peninsula|cape|headland|"
    r"park|reserve|refuge|wilderness|forest|woodland|"
    r"mountain|range|ridge|massif|hill|peak|summit|volcano|caldera|crater|"
    r"plateau|mesa|desert|dune|salt flat|salt pan|playa|"
    r"canyon|gorge|valley|basin|"
    r"glacier|ice field|ice cap|ice sheet|"
    r"bay|gulf|sea|strait|sound|fjord|inlet|estuary|delta)\b"
)
EXCLUDE_RE = (
    r"\b(amusement|theme|water|skate|sports?|business|industrial|office|"
    r"science|technology|trailer|car|caravan|holiday|mobile home|retirement|"
    r"shooting|driving|rifle|eruption|monster|cemetery|stadium|arena|"
    r"mountain hut|ranger|railway|station|hotel|resort|company|team|"
    r"township|municipality|district|county|village|town|city)\b"
)

_PREFIXES = """
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX osm: <https://www.openstreetmap.org/>
PREFIX osmkey: <https://osm2rdf.cs.uni-freiburg.de/rdf/key#>
PREFIX osm2rdf: <https://osm2rdf.cs.uni-freiburg.de/rdf#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
"""

# Two families so an object tagged both ways comes out once, with both keys.
_QUERIES = {
    "wikidata": """
SELECT ?x ?qid ?wp ?env ?area WHERE {
  VALUES ?t { osm:way osm:relation }
  ?x rdf:type ?t .
  ?x osmkey:wikidata ?qid .
  OPTIONAL { ?x osmkey:wikipedia ?wp }
  ?x geo:hasGeometry/geo:asWKT ?wkt .
  OPTIONAL { ?x osm2rdf:area ?area }
  BIND(geof:envelope(?wkt) AS ?env)
} ORDER BY ?x""",
    "wikipedia": """
SELECT ?x ?qid ?wp ?env ?area WHERE {
  VALUES ?t { osm:way osm:relation }
  ?x rdf:type ?t .
  ?x osmkey:wikipedia ?wp .
  FILTER NOT EXISTS { ?x osmkey:wikidata ?any }
  BIND("" AS ?qid)
  ?x geo:hasGeometry/geo:asWKT ?wkt .
  OPTIONAL { ?x osm2rdf:area ?area }
  BIND(geof:envelope(?wkt) AS ?env)
} ORDER BY ?x""",
}

_OSM_IRI = re.compile(r"<https://www\.openstreetmap\.org/(way|relation)/(\d+)>")
_QID = re.compile(r"(Q\d+)")
_WP_IRI = re.compile(r"<https://([a-z\-]+)\.wikipedia\.org/wiki/(.+)>$")
_WP_LITERAL = re.compile(r'^"?([a-z\-]{2,12}):(.+?)"?(?:\^\^<[^>]*>)?$')
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def extract_path(month, cache_dir=CACHE_DIR):
    return os.path.join(cache_dir, f"extent-{month}.parquet")


def _this_month():
    return date.today().strftime("%Y-%m")


def _months_back(n, start=None):
    y, m = map(int, (start or _this_month()).split("-"))
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


# ── QLever ────────────────────────────────────────────────────

def _sparql_tsv(query):
    params = urllib.parse.urlencode({"query": _PREFIXES + query})
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            req = urllib.request.Request(
                f"{QLEVER_URL}?{params}",
                headers={"User-Agent": USER_AGENT, "Accept": "text/tab-separated-values"})
            with urllib.request.urlopen(req, timeout=900) as resp:
                return resp.read().decode("utf-8", "replace")
        except (urllib.error.URLError, OSError) as e:
            last = e
            wait = min(30 * 2 ** attempt, 300)
            print(f"  QLever: {getattr(e, 'code', e)}; waiting {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"QLever gave up: {last}")


def _wikipedia(value):
    """(lang, Title_with_underscores) from osm2rdf's wikipedia value."""
    if not value:
        return None, None
    m = _WP_IRI.match(value)
    if m:
        lang, title = m.group(1), urllib.parse.unquote(m.group(2))
    else:
        m = _WP_LITERAL.match(value)
        if not m:
            return None, None
        lang, title = m.group(1), m.group(2)
    title = title.strip().replace(" ", "_")
    if not title:
        return None, None
    return lang, title[0].upper() + title[1:]


def _km_scale(lat):
    import math
    return 111.32 * math.cos(math.radians(lat)), 110.57


def _bbox(envelope):
    nums = [float(v) for v in _NUMBER.findall(envelope.split("^^")[0])]
    if len(nums) < 2:
        return None
    xs, ys = nums[0::2], nums[1::2]
    return min(xs), min(ys), max(xs), max(ys)


def _parse_rows(tsv, rows):
    """Append one dict per TSV data line to rows; returns lines read."""
    n = 0
    reader = csv.reader(tsv.splitlines(), delimiter="\t", quoting=csv.QUOTE_NONE)
    header = next(reader, None)
    if not header or header[0] != "?x":
        raise RuntimeError(f"unexpected QLever answer: {tsv[:200]!r}")
    for fields in reader:
        if len(fields) < 5:
            continue
        n += 1
        m = _OSM_IRI.match(fields[0])
        bbox = _bbox(fields[3]) if fields[3] else None
        if not m or bbox is None:
            continue
        xmin, ymin, xmax, ymax = bbox
        kx, ky = _km_scale((ymin + ymax) / 2)
        # An envelope wider than a hemisphere straddles the antimeridian and
        # says nothing about the feature's true width: no extent, flagged.
        width = xmax - xmin
        wraps = width > 180
        q = _QID.search(fields[1])
        lang, title = _wikipedia(fields[2])
        area = None
        if fields[4]:
            try:
                area = float(fields[4].split("^^")[0].strip('"')) / 1e6
            except ValueError:
                area = None
        rows.append({
            "osm_type": m.group(1), "osm_id": int(m.group(2)),
            "qid": q.group(1) if q else None, "wiki_lang": lang, "wiki_title": title,
            "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "antimeridian": wraps,
            "extent_km": None if wraps else max(width * kx, (ymax - ymin) * ky),
            "area_km2": area if area and area > 0 else None,
        })
    return n


def extract(month=None, cache_dir=CACHE_DIR, force=False):
    """Pull every sized, Wikipedia-linked OSM object into this month's extract."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    month = month or _this_month()
    out = extract_path(month, cache_dir)
    if os.path.exists(out) and not force:
        print(f"  {out} exists; skipping")
        return out

    start = time.time()
    rows = []
    for name, query in _QUERIES.items():
        offset = 0
        while True:
            t0 = time.time()
            tsv = _sparql_tsv(f"{query} LIMIT {PAGE_ROWS} OFFSET {offset}")
            n = _parse_rows(tsv, rows)
            print(f"  {name}: offset {offset:,} → {n:,} rows ({time.time() - t0:.0f}s, "
                  f"{len(rows):,} kept so far)")
            offset += n
            if n < PAGE_ROWS:
                break
            time.sleep(1)

    if not rows:
        raise RuntimeError("QLever returned no rows")

    def col(key, typ):
        return pa.array([r[key] for r in rows], type=typ)

    table = pa.table({
        "osm_type": col("osm_type", pa.string()),
        "osm_id": col("osm_id", pa.int64()),
        "qid": col("qid", pa.string()),
        "wiki_lang": col("wiki_lang", pa.string()),
        "wiki_title": col("wiki_title", pa.string()),
        "xmin": col("xmin", pa.float64()),
        "ymin": col("ymin", pa.float64()),
        "xmax": col("xmax", pa.float64()),
        "ymax": col("ymax", pa.float64()),
        "antimeridian": col("antimeridian", pa.bool_()),
        "extent_km": col("extent_km", pa.float32()),
        "area_km2": col("area_km2", pa.float32()),
    }).replace_schema_metadata({
        "source": QLEVER_URL, "month": month,
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_seconds": str(int(time.time() - start)),
    })
    os.makedirs(cache_dir, exist_ok=True)
    pq.write_table(table, out + ".tmp", compression="zstd")
    os.replace(out + ".tmp", out)
    print(f"  {len(rows):,} OSM objects in {(time.time() - start) / 60:.1f}m → {out} "
          f"({os.path.getsize(out) / 2**20:.1f} MiB)")
    return out


# ── the build's side ──────────────────────────────────────────

def ensure_extract(cache_dir=CACHE_DIR, url_base=CACHE_URL_BASE):
    """Path of the newest extract within LOOKBACK_MONTHS, fetching it from
    the cache location when the local dir lacks it; None when there is none."""
    for month in _months_back(LOOKBACK_MONTHS):
        path = extract_path(month, cache_dir)
        if os.path.exists(path):
            return path
        if not url_base:
            continue
        url = f"{url_base}/extent-{month}.parquet"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=300) as resp:
                os.makedirs(cache_dir, exist_ok=True)
                with open(path + ".tmp", "wb") as f:
                    while chunk := resp.read(1 << 20):
                        f.write(chunk)
            os.replace(path + ".tmp", path)
            print(f"  fetched {url}")
            return path
        except (urllib.error.URLError, OSError) as e:
            if os.path.exists(path + ".tmp"):
                os.remove(path + ".tmp")
            print(f"  {url}: {getattr(e, 'code', e)}")
    return None


def sizes_by_page(db, extract_file, pages_table):
    """page_id → (extent_km, area_km2, osm_source), largest extent per page.

    `pages_table` has page_id, qid, title (underscore form) and osm_relation
    (P402, nullable). A page matches an OSM object by QID, by English title,
    or by the relation id Wikidata names for it. Objects without a usable
    extent (point-sized envelopes, antimeridian spans) do not count as a
    match, so such a page keeps null size fields.
    """
    rows = db.execute(f"""
        WITH osm AS (SELECT * FROM read_parquet('{extract_file}') WHERE extent_km > 0),
        hits AS (
            SELECT g.page_id, o.osm_type, o.osm_id, o.extent_km, o.area_km2
            FROM {pages_table} g JOIN osm o ON o.qid = g.qid
            UNION ALL
            SELECT g.page_id, o.osm_type, o.osm_id, o.extent_km, o.area_km2
            FROM {pages_table} g JOIN osm o
              ON o.wiki_lang = 'en' AND o.wiki_title = g.title
            UNION ALL
            SELECT g.page_id, o.osm_type, o.osm_id, o.extent_km, o.area_km2
            FROM {pages_table} g JOIN osm o
              ON o.osm_type = 'relation' AND o.osm_id = g.osm_relation
        )
        SELECT page_id, osm_type, osm_id, extent_km, area_km2
        FROM (SELECT *, row_number() OVER (
                  PARTITION BY page_id ORDER BY extent_km DESC, osm_id) AS rn
              FROM hits)
        WHERE rn = 1
    """).fetchall()
    return {pid: (extent, area, f"{typ}/{oid}") for pid, typ, oid, extent, area in rows}


def report(source, top, extract_file, pageviews_paths):
    import duckdb

    db = duckdb.connect()
    db.execute(f"""
        CREATE VIEW pages AS
        SELECT page_id, qid, replace(label, ' ', '_') AS title, label,
               NULL::BIGINT AS osm_relation, inlink_count, instance_of
        FROM read_parquet('{source}')
    """)
    rank = {}
    if pageviews_paths:
        from pageviews import median_views
        rank = median_views(db, list(pageviews_paths), "pages")
    sized = sizes_by_page(db, extract_file, "pages")
    everything = db.execute("SELECT page_id, label, instance_of, inlink_count FROM pages").fetchall()
    print(f"  {len(sized):,} of {len(everything):,} rows match an OSM way or relation "
          f"({100 * len(sized) / len(everything):.1f}%)")

    natural = [r for r in everything
               if re.search(CLASS_RE, r[2].lower()) and not re.search(EXCLUDE_RE, r[2].lower())]
    natural.sort(key=lambda r: (-(rank.get(r[0], 0)), -r[3], r[0]))
    chosen = natural[:top]
    with_extent = [r for r in chosen if r[0] in sized]
    with_area = [r for r in with_extent if sized[r[0]][1] is not None]
    print(f"  top {len(chosen):,} natural-class rows by pageviews: "
          f"{len(with_extent):,} with extent_km ({100 * len(with_extent) / max(len(chosen), 1):.0f}%), "
          f"{len(with_area):,} with area_km2 ({100 * len(with_area) / max(len(chosen), 1):.0f}%)")
    for pid, label, *_ in chosen[:12]:
        s = sized.get(pid)
        print(f"    {label:<32} {'' if not s else f'{s[0]:8.1f} km  {s[1] or 0:10.1f} km²  {s[2]}'}")
    print("  unmatched among the top 40:",
          ", ".join(label for pid, label, *_ in chosen[:40] if pid not in sized))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--extract", action="store_true", help="write this month's extract")
    parser.add_argument("--month", help="YYYY-MM the extract is filed under (default: this month)")
    parser.add_argument("--force", action="store_true", help="re-extract an existing month")
    parser.add_argument("--report", action="store_true",
                        help="coverage of the newest extract over a parquet's rows")
    parser.add_argument("--source", default="data/wikipedia_geotagged.parquet")
    parser.add_argument("--top", type=int, default=2000)
    parser.add_argument("--cache-dir", default=CACHE_DIR)
    parser.add_argument("--pageviews-months", type=int, default=12)
    args = parser.parse_args()

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    if args.extract:
        extract(args.month, args.cache_dir, args.force)
    elif args.report:
        from pageviews import CACHE_DIR as PV_DIR, cache_path, ensure_months
        extract_file = ensure_extract(args.cache_dir, url_base=None)
        if not extract_file:
            sys.exit("no extract in the cache dir; run --extract first")
        months = ensure_months(args.pageviews_months, PV_DIR, url_base=None)
        report(args.source, args.top, extract_file, [cache_path(m, PV_DIR) for m in months])
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
