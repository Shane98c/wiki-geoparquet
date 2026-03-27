"""
Data quality checks for wiki-geoparquet outputs.

Validates the GeoParquet file for:
  - Row count within expected range
  - No null geometries, valid coordinate ranges
  - Inlink and page_len distribution sanity
  - gt_type coverage
  - Spot checks for well-known articles
  - GeoParquet metadata present
"""

import json
import os
import sys
import pyarrow.parquet as pq

sys.stdout.reconfigure(line_buffering=True)

PARQUET_FILE = "data/wikipedia_geotagged.parquet"

# Well-known articles that must exist
SPOT_CHECKS = [
    "Eiffel Tower",
    "Statue of Liberty",
    "Great Wall of China",
    "Machu Picchu",
    "Taj Mahal",
    "Colosseum",
    "Sydney Opera House",
]

EXPECTED_ROW_RANGE = (400_000, 1_500_000)


def main():
    print("=" * 60)
    print("wiki-geoparquet — Validate")
    print("=" * 60)

    if not os.path.exists(PARQUET_FILE):
        print(f"FAIL: {PARQUET_FILE} not found")
        sys.exit(1)

    table = pq.read_table(PARQUET_FILE)
    n = len(table)
    passed = 0
    failed = 0

    def check(name, condition, detail=""):
        nonlocal passed, failed
        if condition:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failed += 1
            print(f"  FAIL: {name} — {detail}")

    # Row count
    print(f"\n→ Row count: {n:,}")
    check("Row count in range",
          EXPECTED_ROW_RANGE[0] <= n <= EXPECTED_ROW_RANGE[1],
          f"expected {EXPECTED_ROW_RANGE[0]:,}-{EXPECTED_ROW_RANGE[1]:,}")

    # Geometry column — validate via DuckDB spatial
    import duckdb
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    null_geom = db.execute(f"""
        SELECT count(*) FROM '{PARQUET_FILE}' WHERE geometry IS NULL
    """).fetchone()[0]
    check("No null geometries", null_geom == 0, f"{null_geom} nulls")

    bad_coords = db.execute(f"""
        SELECT count(*) FROM '{PARQUET_FILE}'
        WHERE ST_X(geometry) < -180 OR ST_X(geometry) > 180
           OR ST_Y(geometry) < -90  OR ST_Y(geometry) > 90
    """).fetchone()[0]
    check("All coordinates valid", bad_coords == 0, f"{bad_coords} out of range")
    db.close()

    # GeoParquet metadata
    meta = table.schema.metadata or {}
    geo_meta = meta.get(b"geo")
    geo = json.loads(geo_meta) if geo_meta else {}
    geom_col = geo.get("columns", {}).get("geometry", {})

    check("GeoParquet 'geo' metadata present", geo_meta is not None)
    if geo_meta:
        check("Primary column is 'geometry'",
              geo.get("primary_column") == "geometry")
        check("Geometry encoding is WKB",
              geom_col.get("encoding") == "WKB")

    has_bbox = "covering" in geom_col
    if has_bbox:
        print("  INFO: Has covering bbox (spatial predicate pushdown enabled)")
    else:
        print("  INFO: No covering bbox (optional, Hilbert sorting provides spatial locality)")

    # gt_type coverage
    gt_types = table.column("gt_type").to_pylist()
    gt_counts = {}
    for g in gt_types:
        gt_counts[g] = gt_counts.get(g, 0) + 1
    with_gt = sum(1 for g in gt_types if g)
    check("gt_type populated for >30% of articles",
          with_gt > n * 0.3,
          f"only {with_gt:,} ({100 * with_gt / n:.0f}%)")

    print(f"\n  gt_type breakdown (top 15):")
    for gt, cnt in sorted(gt_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"    {gt or '(empty)':<20} {cnt:>8,} ({100 * cnt / n:.1f}%)")

    # Inlink distribution
    inlinks = table.column("inlink_count").to_pylist()
    max_inlink = max(inlinks) if inlinks else 0
    with_inlinks = sum(1 for v in inlinks if v and v > 0)
    above_50 = sum(1 for v in inlinks if v and v >= 50)
    above_500 = sum(1 for v in inlinks if v and v >= 500)
    above_2000 = sum(1 for v in inlinks if v and v >= 2000)
    above_5000 = sum(1 for v in inlinks if v and v >= 5000)

    check("Top article has >10K inlinks", max_inlink > 10_000,
          f"max is {max_inlink:,}")
    check(">50% articles have inlinks", with_inlinks > n * 0.5,
          f"only {with_inlinks:,} ({100 * with_inlinks / n:.0f}%)")

    print(f"\n  Inlink distribution (PMTiles zoom tier thresholds):")
    print(f"    >= 5000 (zoom 0-1):   {above_5000:>8,}")
    print(f"    >= 2000 (zoom 2-3):   {above_2000:>8,}")
    print(f"    >= 500  (zoom 4-6):   {above_500:>8,}")
    print(f"    >= 50   (zoom 7-10):  {above_50:>8,}")
    print(f"    all     (zoom 11-14): {n:>8,}")

    # Page length distribution
    page_lens = table.column("page_len").to_pylist()
    avg_len = sum(page_lens) / n if n else 0
    print(f"\n  Page length: avg {avg_len:,.0f} bytes, "
          f"max {max(page_lens):,} bytes")

    # QID and image coverage
    qids = table.column("qid").to_pylist()
    images = table.column("image_url").to_pylist()
    with_qid = sum(1 for q in qids if q)
    with_image = sum(1 for i in images if i)
    print(f"\n  Coverage:")
    print(f"    With QID:    {with_qid:>8,} ({100 * with_qid / n:.0f}%)")
    print(f"    With image:  {with_image:>8,} ({100 * with_image / n:.0f}%)")
    print(f"    With inlinks:{with_inlinks:>8,} ({100 * with_inlinks / n:.0f}%)")

    # Spot checks
    labels = set(table.column("label").to_pylist())
    print(f"\n→ Spot checks:")
    for name in SPOT_CHECKS:
        check(f"'{name}' exists", name in labels, "not found")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All checks passed!")


if __name__ == "__main__":
    main()
