# wiki-geoparquet

English Wikipedia main-namespace articles with primary Earth coordinates, as
[GeoParquet](https://geoparquet.org/) +
[PMTiles](https://docs.protomaps.com/pmtiles/).

Coordinates, inlink counts, article length, Wikidata QIDs, image links, and
descriptions. Updated monthly from Wikipedia SQL dumps.

## Demo

[Live map](https://shane98c.github.io/wiki-geoparquet/demo.html) — browse and search all articles on a globe.

## Downloads

Grab the latest release from [GitHub Releases](https://github.com/Shane98c/wiki-geoparquet/releases/latest).
The data files are gitignored — download them into `data/` for the local examples below.

| File                          | Description                                                    |
| ----------------------------- | -------------------------------------------------------------- |
| `wikipedia_geotagged.parquet` | GeoParquet, Hilbert-sorted with bbox covering                  |
| `wikipedia_geotagged.pmtiles` | Vector tiles, auto-zoom with overzoom, drops by article length |

Files are also available on R2 with CORS for browser use:
- [`wikipedia_geotagged.parquet`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.parquet)
- [`wikipedia_geotagged.pmtiles`](https://pub-016504dd3a4d419a9c17a8939840935e.r2.dev/v1/wikipedia_geotagged.pmtiles)

## Schema

| Column          | Type      | Description                                                   |
| --------------- | --------- | ------------------------------------------------------------- |
| `geometry`      | WKB Point | WGS84 coordinates                                             |
| `page_id`       | int32     | Wikipedia page ID                                             |
| `qid`           | string    | Wikidata QID (e.g. Q90) — use for joins with Wikidata         |
| `label`         | string    | Article title                                                 |
| `description`   | string    | Short description from `wikibase-shortdesc`                   |
| `gt_type`       | string    | Wikipedia geo classification (city, mountain, landmark, etc.) |
| `page_len`      | int32     | Article length in bytes                                       |
| `inlink_count`  | int32     | Number of namespace-0 pagelinks pointing here                 |
| `wikipedia_url` | string    | Full article URL                                              |
| `image_url`     | string    | Wikimedia Commons image URL                                   |
| `bbox`          | struct    | Covering bbox for spatial predicate pushdown                  |

## Quick start

### Query with DuckDB

```sql
INSTALL spatial; LOAD spatial;

-- Find the most notable geotagged articles
SELECT label, inlink_count, page_len, gt_type, ST_AsText(geometry)
FROM 'data/wikipedia_geotagged.parquet'
ORDER BY inlink_count DESC
LIMIT 20;

-- Spatial query: articles within 50km of Paris
SELECT label, inlink_count, gt_type
FROM 'data/wikipedia_geotagged.parquet'
WHERE ST_DWithin(
    geometry,
    ST_Point(2.3522, 48.8566)::GEOMETRY,
    0.45  -- ~50km in degrees at mid-latitudes
)
ORDER BY inlink_count DESC;
```

### View PMTiles

Open in [pmtiles.io](https://pmtiles.io) by pasting the release URL, or serve
locally:

```bash
npx http-server data/ --cors -p 8081
# Then open: https://pmtiles.io/#url=http://localhost:8081/wikipedia_geotagged.pmtiles
```

### Use in MapLibre

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
   primary Earth coordinates, filtering to main-namespace non-redirect articles
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
