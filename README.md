# wiki-geoparquet

English Wikipedia main-namespace articles with Earth coordinates, as
[GeoParquet](https://geoparquet.org/) +
[PMTiles](https://docs.protomaps.com/pmtiles/).

Coordinates, inlink counts, article length, Wikidata QIDs, image links, and
descriptions. Updated monthly from Wikipedia SQL dumps.

## Data

All files are on R2 with CORS enabled — query directly from the browser or any
Parquet-aware tool:

| File                                                                                                                | Description                                                     |
| ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| [`wikipedia_geotagged.parquet`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.parquet) | GeoParquet, Hilbert-sorted with bbox covering                   |
| [`wikipedia_geotagged.pmtiles`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.pmtiles) | Vector tiles, auto-zoom with overzoom, drops by article length  |
| [`wikipedia_search.parquet`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_search.parquet)       | Lightweight search index (label + coordinates, sorted by label) |

Pinned versions are also available on
[GitHub Releases](https://github.com/Shane98c/wiki-geoparquet/releases/latest).

## Schema

| Column          | Type      | Description                                                   |
| --------------- | --------- | ------------------------------------------------------------- |
| `geometry`      | WKB Point | WGS84 coordinates                                             |
| `page_id`       | int32     | Wikipedia page ID                                             |
| `qid`           | string    | Wikidata QID (e.g. Q90) — use for joins with Wikidata         |
| `label`         | string    | Article title                                                 |
| `description`   | string    | Short description from `wikibase-shortdesc`                   |
| `gt_type`       | string    | Wikipedia geo classification (city, mountain, landmark, etc.) |
| `gt_primary`    | bool      | Whether coordinates are the article's primary geo_tag         |
| `page_len`      | int32     | Article length in bytes                                       |
| `inlink_count`  | int32     | Number of namespace-0 pagelinks pointing here                 |
| `wikipedia_url` | string    | Full article URL                                              |
| `image_url`     | string    | Wikimedia Commons image URL                                   |
| `bbox`          | struct    | Covering bbox for spatial predicate pushdown                  |

The PMTiles carry a subset of these properties (no `geometry`, `qid`,
`wikipedia_url`, `image_url`, or `bbox`). Reconstruct URLs client-side:
- Article: `https://en.wikipedia.org/?curid={page_id}`
- Image: `https://commons.wikimedia.org/wiki/Special:FilePath/{image}`

## Quick start

### Query with DuckDB

```sql
INSTALL spatial; LOAD spatial;

-- Find the most notable geotagged articles
SELECT label, inlink_count, page_len, gt_type, ST_AsText(geometry)
FROM 'https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.parquet'
ORDER BY inlink_count DESC
LIMIT 20;

-- Spatial query: articles within 50km of Paris
SELECT label, inlink_count, gt_type
FROM 'https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.parquet'
ORDER BY inlink_count DESC;
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
        url: "pmtiles://https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.pmtiles",
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
            1.5,
            100,
            10,
          ],
          "circle-color": "#4264fb",
        },
      },
    ],
  },
});
```

## How it works

1. Streams 5 Wikipedia SQL dump files (~12 GB) and extracts geotagged pages with
   Earth coordinates (preferring primary, falling back to non-primary),
   filtering to main-namespace non-redirect articles
2. Joins with page metadata, Wikidata properties, and pagelinks-based inlink
   counts
3. Writes Hilbert-sorted GeoParquet with bbox covering via DuckDB spatial
4. Pipes DuckDB to [tippecanoe](https://github.com/felt/tippecanoe) for PMTiles
   with attribute-based feature dropping

See the [Makefile](Makefile) for build steps.

## Data source

All data from
[English Wikipedia SQL dumps](https://dumps.wikimedia.org/enwiki/latest/),
released under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

## License

Code: [MIT](LICENSE) Data outputs:
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (derived from
Wikipedia/Wikidata)
