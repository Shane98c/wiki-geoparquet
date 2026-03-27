# wiki-geoparquet

Every geotagged English Wikipedia article as [GeoParquet](https://geoparquet.org/) + [PMTiles](https://docs.protomaps.com/pmtiles/).

**1,236,747 articles** with coordinates, inlink counts, article length, Wikidata QIDs, images, and descriptions. Updated monthly from Wikipedia SQL dumps.

## Downloads

Grab the latest release from [GitHub Releases](../../releases/latest):

| File | Size | Description |
|------|------|-------------|
| `wikipedia_geotagged.parquet` | ~76 MB | GeoParquet, Hilbert-sorted for spatial queries |
| `wikipedia_geotagged.pmtiles` | ~282 MB | Vector tiles, auto-zoom with overzoom, drops by article length |

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `geometry` | WKB Point | WGS84 coordinates |
| `page_id` | int32 | Wikipedia page ID |
| `qid` | string | Wikidata QID (e.g. Q90) — use for joins with Wikidata |
| `label` | string | Article title |
| `description` | string | Wikidata short description |
| `gt_type` | string | Wikipedia geo classification (city, mountain, landmark, etc.) |
| `page_len` | int32 | Article length in bytes |
| `inlink_count` | int32 | Number of Wikipedia articles linking here |
| `wikipedia_url` | string | Full article URL |
| `image_url` | string | Wikimedia Commons image URL |

## Quick start

### Query with DuckDB

```sql
INSTALL spatial; LOAD spatial;

-- Find the most notable geotagged articles
SELECT label, inlink_count, page_len, gt_type, ST_AsText(geometry)
FROM 'wikipedia_geotagged.parquet'
ORDER BY inlink_count DESC
LIMIT 20;

-- Spatial query: articles within 50km of Paris
SELECT label, inlink_count, gt_type
FROM 'wikipedia_geotagged.parquet'
WHERE ST_DWithin(
    geometry,
    ST_Point(2.3522, 48.8566)::GEOMETRY,
    0.45  -- ~50km in degrees at mid-latitudes
)
ORDER BY inlink_count DESC;
```

### View PMTiles

Open in [pmtiles.io](https://pmtiles.io) by pasting the release URL, or serve locally:

```bash
npx http-server data/ --cors -p 8081
# Then open: https://pmtiles.io/#url=http://localhost:8081/wikipedia_geotagged.pmtiles
```

### Use in MapLibre

```js
import { Protocol } from 'pmtiles';

let protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

const map = new maplibregl.Map({
  style: {
    sources: {
      wikipedia: {
        type: 'vector',
        url: 'pmtiles://https://github.com/.../releases/latest/download/wikipedia_geotagged.pmtiles',
      }
    },
    layers: [{
      id: 'articles',
      source: 'wikipedia',
      'source-layer': 'wikipedia',
      type: 'circle',
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['get', 'inlink_count'], 0, 2, 10000, 8],
        'circle-color': '#4264fb',
      }
    }]
  }
});
```

## Build from source

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- [DuckDB CLI](https://duckdb.org/docs/installation/) (with spatial extension)
- [tippecanoe](https://github.com/felt/tippecanoe) (build from main — `--drop-by-attribute-as-needed` is merged but not yet in a release)

```bash
# Install tippecanoe from source
git clone https://github.com/felt/tippecanoe.git
cd tippecanoe && make -j && sudo make install
```

### Run

```bash
git clone https://github.com/Shane98c/wiki-geoparquet.git
cd wiki-geoparquet
uv sync

# Download Wikipedia dumps (~12 GB)
make download

# Full build: extract → tiles → validate
make build

# Publish to GitHub Releases
make release
```

The full pipeline takes ~2 hours. The extract step streams ~12 GB of Wikipedia SQL dumps and builds the GeoParquet. The tiles step converts to PMTiles via tippecanoe.

### Individual steps

```bash
make download    # Fetch Wikipedia SQL dumps
make extract     # Dumps → GeoParquet (~107 min)
make tiles       # GeoParquet → PMTiles via tippecanoe
make validate    # Run quality checks
make clean       # Remove generated files
```

## How it works

### GeoParquet

1. Streams 5 Wikipedia SQL dump files (geo_tags, page, page_props, linktarget, pagelinks)
2. Extracts all articles with Earth coordinates in the main namespace
3. Joins with page metadata, Wikidata properties, and inbound link counts
4. Writes GeoParquet via DuckDB spatial with Hilbert curve sorting for spatial locality

### PMTiles

Uses tippecanoe with `--drop-by-attribute-as-needed=page_len` and `-r1`:

- Uses `-zg` to auto-pick the lowest max zoom where all features fit; clients overzoom beyond that
- At lower zooms, when tiles exceed 500KB, features with the shortest articles are dropped first
- No spatial drop rate (`-r1`) — every feature that survives earned its spot by article quality
- `--drop-by-attribute-as-needed` was [merged to tippecanoe main](https://github.com/felt/tippecanoe/pull/384) but not yet in a release

## Data source

All data comes from [English Wikipedia SQL dumps](https://dumps.wikimedia.org/enwiki/latest/), released under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

## License

Code: [MIT](LICENSE)
Data outputs: [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (derived from Wikipedia/Wikidata)
