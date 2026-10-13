"""
Monthly English Wikipedia pageviews, one small Parquet per month.

Source: the Wikimedia pageview_complete monthly dumps, human traffic only
(`pageviews-YYYYMM-user.bz2`, every project, ~6 GB). One line per
(project, title, page_id, access method):

    en.wikipedia Mount_Rushmore 61742 desktop 71520 A2101B1987...

A month's extract keeps the en.wikipedia rows, sums views over access methods
per page_id (per title when the dump has no page_id) and lands in
data/pageviews/en/YYYY-MM.parquet — tens of MB, the shape the build joins.

The dump is sorted by project, so only the en.wikipedia third of the file is
read: bzip2 blocks are independent, and a binary search over HTTP ranges
finds the block where en.wikipedia begins. Decoding stops at the first
project after it.

    pageviews.py --month 2026-09          extract one month (no-op if cached)
    pageviews.py --list                   months the dump server has
    pageviews.py --check PARQUET [-n 12]  join the cache to a parquet, report
"""

import argparse
import bz2
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date

sys.stdout.reconfigure(line_buffering=True)

DUMP_BASE = "https://dumps.wikimedia.org/other/pageview_complete/monthly"
CACHE_DIR = "data/pageviews/en"
# Where the pageviews workflow publishes each month's extract; the build
# fetches the months it lacks from here.
CACHE_URL_BASE = "https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/pageviews/en"
USER_AGENT = "wiki-geoparquet/1.0 (github.com/Shane98c/wiki-geoparquet)"
PROJECT = b"en.wikipedia"
PROJECT_PREFIX = PROJECT + b" "
# Titles in these namespaces are not articles; the page table joins on
# namespace 0 only, so their rows would never match.
_NAMESPACE_PREFIXES = tuple(
    ns.encode() + b":" for ns in (
        "Talk", "User", "User_talk", "Wikipedia", "Wikipedia_talk", "File",
        "File_talk", "MediaWiki", "MediaWiki_talk", "Template", "Template_talk",
        "Help", "Help_talk", "Category", "Category_talk", "Portal", "Portal_talk",
        "Draft", "Draft_talk", "Module", "Module_talk", "TimedText", "Special",
        "Media", "Gadget", "Gadget_definition", "Event", "Event_talk",
    )
)

_BLOCK_MAGIC = b"\x31\x41\x59\x26\x53\x59"
_PROBE_BYTES = 2 * 1024 * 1024
_CHUNK = 1 << 20
_INT32_MAX = 2**31 - 1


def month_url(month):
    year, mm = month.split("-")
    return f"{DUMP_BASE}/{year}/{month}/pageviews-{year}{mm}-user.bz2"


def cache_path(month, cache_dir=CACHE_DIR):
    return os.path.join(cache_dir, f"{month}.parquet")


def _request(url, headers=None, method="GET"):
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    return urllib.request.Request(url, headers=h, method=method)


def _fetch_range(url, start, length):
    req = _request(url, {"Range": f"bytes={start}-{start + length - 1}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _content_length(url):
    with urllib.request.urlopen(_request(url, method="HEAD"), timeout=60) as resp:
        return int(resp.headers["Content-Length"])


def list_available_months():
    """Months whose -user.bz2 the dump server has, oldest first."""
    months = []
    with urllib.request.urlopen(_request(DUMP_BASE + "/"), timeout=60) as resp:
        years = sorted(set(re.findall(rb'href="(\d{4})/"', resp.read())))
    for year in years[-2:]:
        with urllib.request.urlopen(_request(f"{DUMP_BASE}/{year.decode()}/"), timeout=60) as resp:
            dirs = sorted(set(re.findall(rb'href="(\d{4}-\d{2})/"', resp.read())))
        for d in dirs:
            month = d.decode()
            try:
                _content_length(month_url(month))
            except urllib.error.HTTPError:
                continue
            months.append(month)
    return months


# ── bzip2 block seeking ───────────────────────────────────────

def _shift_bits(data, s):
    """`data` shifted left by `s` bits; the last byte holds partial bits."""
    if s == 0:
        return data
    n = (int.from_bytes(data, "big") << s) & ((1 << (8 * len(data))) - 1)
    return n.to_bytes(len(data), "big")


def _find_block(data, min_index=0):
    """(byte index, bit shift) of the first block magic at or after min_index."""
    best = None
    for s in range(8):
        i = _shift_bits(data, s).find(_BLOCK_MAGIC, min_index)
        if i >= 0 and (best is None or i < best[0]):
            best = (i, s)
    return best


class _BitShiftedBz2:
    """Decompresses a bzip2 stream whose blocks start `shift` bits into `raw`.

    A bzip2 block is self-contained, so a stream rebuilt as the stream header
    plus the bytes from any block boundary decodes from that block on.
    """

    def __init__(self, shift):
        self.shift = shift
        self.carry = b""
        self.dec = bz2.BZ2Decompressor()
        self.pending_header = b"BZh9"

    def feed(self, raw):
        data = self.carry + raw
        shifted = _shift_bits(data, self.shift)
        # The final byte's low bits belong to the next chunk.
        self.carry = data[-1:]
        payload = self.pending_header + shifted[:-1]
        self.pending_header = b""
        return self.dec.decompress(payload)


def _probe_project(url, offset, size):
    """Project named by the first whole line decodable from a block at or after offset."""
    length = min(_PROBE_BYTES, size - offset)
    data = _fetch_range(url, offset, length)
    found = _find_block(data)
    if found is None:
        return None, None
    i, s = found
    dec = bz2.BZ2Decompressor()
    try:
        out = dec.decompress(b"BZh9" + _shift_bits(data[i:], s)[:-1])
    except OSError:
        return None, None
    lines = out.split(b"\n")
    if len(lines) < 3:
        return None, None
    return lines[1].split(b" ", 1)[0], offset + i


def seek_project_start(url, size, project=PROJECT):
    """Byte offset of a block that starts before `project`'s rows and lies
    within a probe of them; None when the search cannot be trusted."""
    lo, hi = 0, size
    lo_block = 0
    while hi - lo > _PROBE_BYTES:
        mid = (lo + hi) // 2
        found, block = _probe_project(url, mid, size)
        if found is None:
            hi = mid
            continue
        if found < project:
            lo, lo_block = mid, block
        else:
            hi = mid
    return lo_block


# ── one month ─────────────────────────────────────────────────

def extract_month(month, cache_dir=CACHE_DIR, force=False):
    """Stream one month's dump and write its en.wikipedia extract."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = cache_path(month, cache_dir)
    if os.path.exists(out) and not force:
        print(f"  {out} exists; skipping")
        return out

    url = month_url(month)
    size = _content_length(url)
    print(f"  {url} ({size / 2**30:.1f} GiB)")
    start = time.time()

    offset = seek_project_start(url, size)
    print(f"  {PROJECT.decode()} block search landed at byte {offset:,} "
          f"({100 * offset / size:.0f}% of the file, {time.time() - start:.0f}s)")

    first = _fetch_range(url, offset, _PROBE_BYTES)
    i, shift = _find_block(first)
    offset += i
    first = first[i:]

    by_page = {}
    by_title = {}
    lines_seen = 0
    project_lines = 0
    bytes_read = 0
    tail = b""
    seen_project = False
    done = False

    def consume(text):
        nonlocal tail, lines_seen, project_lines, seen_project, done
        buf = tail + text
        cut = buf.rfind(b"\n")
        if cut < 0:
            tail = buf
            return
        tail = buf[cut + 1:]
        for line in buf[:cut].split(b"\n"):
            lines_seen += 1
            if not line.startswith(PROJECT_PREFIX):
                if seen_project:
                    done = True
                    return
                continue
            seen_project = True
            fields = line.split(b" ")
            if len(fields) == 6:
                _, title, page_id, _, views, _ = fields
            elif len(fields) == 5:
                _, title, _, views, _ = fields
                page_id = b"null"
            else:
                continue
            if title.startswith(_NAMESPACE_PREFIXES) or title == b"-":
                continue
            try:
                views = int(views)
            except ValueError:
                continue
            project_lines += 1
            if page_id.isdigit():
                pid = int(page_id)
                by_page[pid] = by_page.get(pid, 0) + views
            else:
                by_title[title] = by_title.get(title, 0) + views

    req = _request(url, {"Range": f"bytes={offset}-"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        stream = _BitShiftedBz2(shift)
        # The probe already holds the first bytes; the first line it yields
        # is cut mid-row and is dropped by starting after its newline.
        out_bytes = stream.feed(first)
        consume(out_bytes[out_bytes.find(b"\n") + 1:])
        bytes_read = len(first)
        remaining_skip = _PROBE_BYTES - i
        while not done:
            chunk = resp.read(_CHUNK)
            if not chunk:
                break
            if remaining_skip:
                take = min(remaining_skip, len(chunk))
                chunk = chunk[take:]
                remaining_skip -= take
                if not chunk:
                    continue
            bytes_read += len(chunk)
            try:
                consume(stream.feed(chunk))
            except (OSError, EOFError) as e:
                if seen_project:
                    print(f"  decoder stopped at the end of the stream: {e}")
                    break
                raise
            if bytes_read % (256 * _CHUNK) < _CHUNK:
                print(f"  ...{bytes_read / 2**30:.2f} GiB read, {project_lines:,} "
                      f"{PROJECT.decode()} rows, {len(by_page):,} pages "
                      f"({(time.time() - start) / 60:.1f}m)")

    if not seen_project:
        raise RuntimeError(f"no {PROJECT.decode()} rows found in {url}")
    if not done:
        print("  WARNING: stream ended inside the project block; the month may be partial")

    pids = sorted(by_page)
    titles = sorted(by_title)
    table = pa.table({
        "month": pa.array([month] * (len(pids) + len(titles)), type=pa.string()),
        "page_id": pa.array(pids + [None] * len(titles), type=pa.int32()),
        "title": pa.array([None] * len(pids) + [t.decode("utf-8", "replace") for t in titles],
                          type=pa.string()),
        "views": pa.array([by_page[p] for p in pids] + [by_title[t] for t in titles],
                          type=pa.int64()),
    })
    os.makedirs(cache_dir, exist_ok=True)
    tmp = out + ".tmp"
    table = table.replace_schema_metadata({
        "source": url, "project": PROJECT.decode(), "complete": str(done).lower(),
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out)
    elapsed = time.time() - start
    print(f"  {month}: {project_lines:,} rows → {len(pids):,} pages + {len(titles):,} "
          f"title-only rows, {bytes_read / 2**30:.2f} GiB read in {elapsed / 60:.1f}m "
          f"→ {out} ({os.path.getsize(out) / 2**20:.1f} MiB)")
    return out


# ── the build's side ──────────────────────────────────────────

def recent_months(n, today=None):
    """The n calendar months before the current one, oldest first."""
    today = today or date.today()
    y, m = today.year, today.month
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append(f"{y:04d}-{m:02d}")
    return sorted(out)


def ensure_months(n, cache_dir=CACHE_DIR, url_base=CACHE_URL_BASE):
    """Paths of the cached extracts for the last n months, fetching any the
    cache location has that the local dir lacks. Months nobody has are
    reported and skipped; the build goes on with what there is."""
    have = []
    missing = []
    for month in recent_months(n):
        path = cache_path(month, cache_dir)
        if not os.path.exists(path) and url_base:
            url = f"{url_base}/{month}.parquet"
            try:
                os.makedirs(cache_dir, exist_ok=True)
                with urllib.request.urlopen(_request(url), timeout=120) as resp, \
                        open(path + ".tmp", "wb") as f:
                    while chunk := resp.read(_CHUNK):
                        f.write(chunk)
                os.replace(path + ".tmp", path)
                print(f"  fetched {url}")
            except (urllib.error.URLError, OSError) as e:
                if os.path.exists(path + ".tmp"):
                    os.remove(path + ".tmp")
                print(f"  {url}: {getattr(e, 'code', e)}")
        if os.path.exists(path):
            have.append(month)
        else:
            missing.append(month)
    if missing:
        print(f"  WARNING: no pageviews extract for {', '.join(missing)}; "
              f"using {len(have)} of {n} months")
    return have


def median_views(db, month_paths, pages_table):
    """page_id → median monthly views over the cached months.

    `pages_table` is a DuckDB-visible relation with page_id and title (the
    dump's underscore form). A month's total for a page is its page_id rows
    plus any title-only rows carrying its title.
    """
    if not month_paths:
        return {}
    files = ", ".join(f"'{p}'" for p in month_paths)
    rows = db.execute(f"""
        WITH pv AS (SELECT * FROM read_parquet([{files}])),
        monthly AS (
            SELECT g.page_id, pv.month, sum(pv.views) AS views
            FROM pv JOIN {pages_table} g ON pv.page_id = g.page_id
            GROUP BY 1, 2
            UNION ALL
            SELECT g.page_id, pv.month, sum(pv.views) AS views
            FROM pv JOIN {pages_table} g ON pv.page_id IS NULL AND pv.title = g.title
            GROUP BY 1, 2
        ),
        totals AS (SELECT page_id, month, sum(views) AS views FROM monthly GROUP BY 1, 2)
        SELECT page_id, median(views) FROM totals GROUP BY 1
    """).fetchall()
    return {pid: min(int(round(v)), _INT32_MAX) for pid, v in rows}


def _check(parquet, n, cache_dir):
    import duckdb

    months = ensure_months(n, cache_dir, url_base=None)
    print(f"  months: {', '.join(months) or 'none'}")
    db = duckdb.connect()
    db.execute(f"""CREATE VIEW pages AS
        SELECT page_id, replace(label, ' ', '_') AS title, label, inlink_count
        FROM read_parquet('{parquet}')""")
    paths = [cache_path(m, cache_dir) for m in months]
    views = median_views(db, paths, "pages")
    total = db.execute("SELECT count(*) FROM pages").fetchone()[0]
    print(f"  rows with pageviews: {len(views):,} of {total:,} ({100 * len(views) / total:.1f}%)")
    for name in ("Mount Rushmore", "Coon Rapids Dam", "Black Hills", "Mount Rainier",
                 "Minneapolis", "Glen Canyon Dam", "Bear Butte", "Medicine Rocks State Park"):
        row = db.execute("SELECT page_id FROM pages WHERE label = ?", [name]).fetchone()
        print(f"    {name:<28} {views.get(row[0]) if row else 'not in parquet'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", help="YYYY-MM to extract")
    parser.add_argument("--list", action="store_true", help="months the dump server has")
    parser.add_argument("--check", metavar="PARQUET", help="join the cache to a parquet and report")
    parser.add_argument("-n", "--months", type=int, default=12, help="months to use with --check")
    parser.add_argument("--cache-dir", default=CACHE_DIR)
    parser.add_argument("--force", action="store_true", help="re-extract a cached month")
    args = parser.parse_args()

    if args.list:
        print("\n".join(list_available_months()))
    elif args.month:
        if not re.fullmatch(r"\d{4}-\d{2}", args.month):
            parser.error("--month wants YYYY-MM")
        extract_month(args.month, args.cache_dir, args.force)
    elif args.check:
        _check(args.check, args.months, args.cache_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
