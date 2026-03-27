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

import duckdb

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

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    n = db.execute(f"SELECT count(*) FROM '{PARQUET_FILE}'").fetchone()[0]
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

    # Geometry validation
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

    # GeoParquet metadata
    geo_meta_row = db.execute(f"""
        SELECT value FROM parquet_kv_metadata('{PARQUET_FILE}') WHERE key = 'geo'
    """).fetchone()
    geo_meta = geo_meta_row[0] if geo_meta_row else None
    geo = json.loads(geo_meta) if geo_meta else {}
    geom_col = geo.get("columns", {}).get("geometry", {})

    check("GeoParquet 'geo' metadata present", geo_meta is not None)
    if geo_meta:
        check("Primary column is 'geometry'",
              geo.get("primary_column") == "geometry")
        check("Geometry encoding is WKB",
              geom_col.get("encoding") == "WKB")

    has_bbox = "covering" in geom_col
    check("Has covering bbox metadata", has_bbox,
          "missing — spatial predicate pushdown will not work")

    if has_bbox:
        bbox_col = db.execute(f"""
            SELECT count(*) FROM parquet_schema('{PARQUET_FILE}') WHERE name = 'bbox'
        """).fetchone()[0]
        check("bbox struct column exists", bbox_col > 0, "covering declared but bbox column missing")

    # gt_type coverage
    gt_rows = db.execute(f"""
        SELECT gt_type, count(*) AS cnt FROM '{PARQUET_FILE}'
        GROUP BY gt_type ORDER BY cnt DESC
    """).fetchall()
    with_gt = sum(cnt for gt, cnt in gt_rows if gt)
    check("gt_type populated for >30% of articles",
          with_gt > n * 0.3,
          f"only {with_gt:,} ({100 * with_gt / n:.0f}%)")

    print(f"\n  gt_type breakdown (top 15):")
    for gt, cnt in gt_rows[:15]:
        print(f"    {gt or '(empty)':<20} {cnt:>8,} ({100 * cnt / n:.1f}%)")

    # Inlink distribution
    inlink_stats = db.execute(f"""
        SELECT
            max(inlink_count),
            count(*) FILTER (WHERE inlink_count > 0),
            count(*) FILTER (WHERE inlink_count >= 50),
            count(*) FILTER (WHERE inlink_count >= 500),
            count(*) FILTER (WHERE inlink_count >= 2000),
            count(*) FILTER (WHERE inlink_count >= 5000)
        FROM '{PARQUET_FILE}'
    """).fetchone()
    max_inlink, with_inlinks, above_50, above_500, above_2000, above_5000 = inlink_stats

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
    page_stats = db.execute(f"""
        SELECT avg(page_len), max(page_len) FROM '{PARQUET_FILE}'
    """).fetchone()
    print(f"\n  Page length: avg {page_stats[0]:,.0f} bytes, "
          f"max {page_stats[1]:,} bytes")

    # QID and image coverage
    coverage = db.execute(f"""
        SELECT
            count(*) FILTER (WHERE qid != ''),
            count(*) FILTER (WHERE image_url != '')
        FROM '{PARQUET_FILE}'
    """).fetchone()
    with_qid, with_image = coverage
    print(f"\n  Coverage:")
    print(f"    With QID:    {with_qid:>8,} ({100 * with_qid / n:.0f}%)")
    print(f"    With image:  {with_image:>8,} ({100 * with_image / n:.0f}%)")
    print(f"    With inlinks:{with_inlinks:>8,} ({100 * with_inlinks / n:.0f}%)")

    # Spot checks
    print(f"\n→ Spot checks:")
    for name in SPOT_CHECKS:
        found = db.execute(f"""
            SELECT count(*) FROM '{PARQUET_FILE}' WHERE label = ?
        """, [name]).fetchone()[0]
        check(f"'{name}' exists", found > 0, "not found")

    db.close()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
    print("All checks passed!")


if __name__ == "__main__":
    main()
