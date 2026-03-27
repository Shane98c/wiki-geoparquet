"""
Build PMTiles from GeoParquet using tippecanoe with attribute-based dropping.

Uses --drop-by-attribute-as-needed to drop features with the lowest page_len
first when tiles exceed the size limit. This operates per zoom level, matching
tippecanoe's standard drop behavior but using article length as the quality
metric instead of spatial density.

Requires tippecanoe built from main (--drop-by-attribute-as-needed is merged but not yet in a release).
"""

import os
import shutil
import subprocess
import sys
import tempfile

sys.stdout.reconfigure(line_buffering=True)

INPUT_FILE = "data/wikipedia_geotagged.parquet"
OUTPUT_FILE = "data/wikipedia_geotagged.pmtiles"
MIN_ZOOM = 0

# tippecanoe binary — requires build from main until a release includes
# --drop-by-attribute-as-needed (https://github.com/felt/tippecanoe)
TIPPECANOE = os.environ.get("TIPPECANOE", "tippecanoe")


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found. Run extract.py first.")
        sys.exit(1)

    if not shutil.which(TIPPECANOE):
        print(f"ERROR: tippecanoe not found: {TIPPECANOE}")
        print("Install tippecanoe or set TIPPECANOE env var to the binary path.")
        sys.exit(1)

    import duckdb

    print("=" * 60)
    print("wiki-geoparquet — Build PMTiles")
    print("=" * 60)

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    total = db.execute(f"SELECT count(*) FROM '{INPUT_FILE}'").fetchone()[0]
    print(f"\n→ {total:,} features")

    with tempfile.TemporaryDirectory() as tmpdir:
        ndjson_path = os.path.join(tmpdir, "features.geojsonl")

        # Step 1: Export GeoParquet → NDJSON GeoJSON via DuckDB
        print(f"\n→ Exporting to NDJSON...")
        db.execute(f"""
            COPY (SELECT * FROM '{INPUT_FILE}')
            TO '{ndjson_path}'
            WITH (FORMAT GDAL, DRIVER 'GeoJSONSeq')
        """)
        db.close()

        ndjson_mb = os.path.getsize(ndjson_path) / (1024 * 1024)
        print(f"  {ndjson_mb:.0f} MB")

        # Step 2: tippecanoe with attribute-based dropping
        print(f"\n→ Running tippecanoe...")
        cmd = [
            TIPPECANOE,
            "-o", OUTPUT_FILE,
            "-f",
            "-l", "wikipedia",
            "--name=Wikipedia Geotagged Articles",
            "--attribution=Wikipedia/Wikidata, CC BY-SA 4.0",
            f"--minimum-zoom={MIN_ZOOM}",
            "-zg",  # auto-pick max zoom; client overzooms beyond
            "--drop-by-attribute-as-needed=page_len",
            "--extend-zooms-if-still-dropping",
            "-r1",  # no spatial drop rate — attribute dropping only
            "-P",  # parallel read
            ndjson_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"ERROR: tippecanoe failed:\n{result.stderr}")
            sys.exit(1)

        if result.stderr:
            for line in result.stderr.strip().split('\n')[-15:]:
                print(f"  {line}")

    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n{'=' * 60}")
    print(f"Written to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")
    print("Done!")


if __name__ == "__main__":
    main()
