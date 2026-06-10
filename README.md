# wiki-geoparquet

English Wikipedia main-namespace articles with Earth coordinates, as
[GeoParquet](https://geoparquet.org/) +
[PMTiles](https://docs.protomaps.com/pmtiles/).

Coordinates from Wikidata P625, enriched with instance type, country, population,
GeoNames IDs, inlink counts, and more. Updated monthly from Wikidata + Wikipedia dumps.

**demo:**
[shane98c.github.io/wiki-geoparquet](https://shane98c.github.io/wiki-geoparquet/)

## Data

All files are on R2 with CORS enabled — query directly from the browser or any
Parquet-aware tool:

| File                                                                                                                | Description                                                                                                           |
| ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [`wikipedia_geotagged.parquet`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_geotagged.parquet) | GeoParquet, Hilbert-sorted with bbox covering                                                                         |
| [`wikipedia_geotagged.pmtiles`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_geotagged.pmtiles) | Vector tiles, auto-zoom with overzoom, drops by article length                                                        |
| [`wikipedia_search.parquet`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_search.parquet)       | Lightweight search index (lowercased label, coords, inlink count), sorted by label for prefix-range row-group pruning |

Pinned versions are also available on
[GitHub Releases](https://github.com/Shane98c/wiki-geoparquet/releases/latest).

## Schema

| Column           | Type           | Description                                              |
| ---------------- | -------------- | -------------------------------------------------------- |
| `geometry`       | WKB Point      | WGS84 coordinates (from Wikidata P625)                   |
| `page_id`        | int32          | Wikipedia page ID                                        |
| `qid`            | string         | Wikidata QID (e.g. Q90)                                  |
| `label`          | string         | Article title                                            |
| `description`    | string         | Short description from `wikibase-shortdesc`              |
| `instance_of`    | string         | Wikidata P31 type label (e.g. "city", "mountain", "museum") |
| `instance_of_qid`| string         | Wikidata P31 QID (e.g. "Q515", "Q8502") — for downstream mapping to custom taxonomies |
| `country`        | string         | Country name from Wikidata P17                           |
| `population`     | int64          | Population from Wikidata P1082 (nullable)                |
| `geonames_id`    | string         | GeoNames ID for cross-referencing                        |
| `elevation`      | int16          | Ground elevation in meters, sampled from Copernicus DEM  |
| `gt_type`        | string         | Wikipedia geo classification (supplementary, from geo_tags) |
| `page_len`       | int32          | Article length in bytes                                  |
| `inlink_count`   | int32          | Number of namespace-0 pagelinks pointing here            |
| `wikipedia_url`  | string         | Full article URL                                         |
| `image_url`      | string         | Wikimedia Commons image URL                              |
| `related_images` | list\<string\> | Additional Wikidata images (P6802)                       |
| `bbox`           | struct         | Covering bbox for spatial predicate pushdown             |

The PMTiles carry `page_id`, `label`, `inlink_count`, and `instance_of_qid` —
the QID lets downstream consumers apply their own category taxonomy via
Mapbox/MapLibre `match` expressions without rebuilding tiles. The demo
enriches popups on click by querying the GeoParquet directly via DuckDB-WASM.
Reconstruct article URLs client-side: `https://en.wikipedia.org/?curid={page_id}`

## Quick start

### Query with DuckDB

```sql
INSTALL spatial; LOAD spatial;

-- Find the most notable geotagged articles
SELECT label, instance_of, country, inlink_count, ST_AsText(geometry)
FROM 'https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_geotagged.parquet'
ORDER BY inlink_count DESC
LIMIT 20;

-- Articles near Paris (48.8566, 2.3522) — ~50km bbox
SELECT label, instance_of, country, inlink_count
FROM 'https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_geotagged.parquet'
WHERE bbox.xmin BETWEEN 1.7 AND 3.0
  AND bbox.ymin BETWEEN 48.4 AND 49.3
ORDER BY inlink_count DESC
LIMIT 20;
```

### Browser search with duckdb-wasm

The `wikipedia_search.parquet` index has columns `label` (lowercased, used for
both sorting and lookup), `lon`, `lat`, and `inlink_count` for ranking. It's
sorted by `label` so a lowercased prefix range lets DuckDB skip row groups and
fetch ~2 MB per query instead of the full ~25 MB file.

```js
// In duckdb-wasm, INSTALL httpfs (or SET builtin_httpfs = false) — without
// this the built-in HTTP handler downloads the whole file on every query.
// See https://github.com/duckdb/duckdb-wasm/issues/2153.
await conn.query("SET builtin_httpfs = false;");

const q = userInput.toLowerCase().replace(/'/g, "''");
await conn.query(`
  SELECT label, lon, lat
  FROM 'https://.../wikipedia_search.parquet'
  WHERE label >= '${q}' AND label < '${q}~'
  ORDER BY inlink_count DESC
  LIMIT 10
`);
```

### Use pmtiles in MapLibre

```js
import { Protocol } from "pmtiles";

let protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  style: {
    sources: {
      wikipedia: {
        type: "vector",
        url: "pmtiles://https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v2/wikipedia_geotagged.pmtiles",
      },
    },
    layers: [
      {
        id: "articles",
        source: "wikipedia",
        "source-layer": "wikipedia",
        type: "circle",
        paint: {
          "circle-radius": [
            "interpolate",
            ["linear"],
            ["sqrt", ["get", "inlink_count"]],
            0,
            2.5,
            280,
            20,
          ],
          "circle-color": "#4264fb",
        },
      },
    ],
  },
});
```

## How it works

1. Streams the Wikidata truthy N-Triples dump (~70 GB) to extract P625
   coordinates and enrichment properties (instance type, country, population,
   GeoNames ID, images)
2. Resolves P31/P17 QIDs to human-readable labels via the Wikidata API
3. Streams 5 Wikipedia SQL dump files (~12 GB) for article metadata, images,
   descriptions, and inlink counts
4. Writes Hilbert-sorted GeoParquet with bbox covering via DuckDB spatial
5. Pipes DuckDB to [tippecanoe](https://github.com/felt/tippecanoe) for PMTiles
   with attribute-based feature dropping

See the [Makefile](Makefile) for build steps.

## Data source

Coordinates and enrichment from
[Wikidata entity dumps](https://dumps.wikimedia.org/wikidatawiki/entities/),
article metadata from
[English Wikipedia SQL dumps](https://dumps.wikimedia.org/enwiki/latest/).
Both released under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

## License

Code: [MIT](LICENSE) Data outputs:
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (derived from
Wikipedia/Wikidata)
