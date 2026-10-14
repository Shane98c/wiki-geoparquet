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

Each release also carries `qid_labels.json`, the QID → English label map the
build resolved for `instance_of` and `country`. The next monthly build seeds
from it and asks the Wikidata API only for QIDs new to the dataset.

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
| `pageviews_month`| int32          | Median monthly human pageviews over the joined months (nullable; see [Pageviews](#pageviews)) |
| `extent_km`      | float32        | Longer side of the feature's OSM bbox in km (nullable; see [Feature size from OSM](#feature-size-from-osm)) |
| `area_km2`       | float32        | Polygon area from OSM for closed ways and multipolygon relations (nullable) |
| `osm_source`     | string         | OSM element the size came from, `relation/4039486` or `way/…` (nullable) |
| `bbox`           | struct         | Covering bbox for spatial predicate pushdown             |

Key-value metadata on the file records `pageviews_months` (the months joined,
comma-separated), `pageviews_months_wanted` and `osm_extent_extract` (the extract file joined).

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

## Pageviews

`pageviews_month` is the median of a page's monthly totals over the last 12
months (`--pageviews-months`), human traffic only. The median is the steady
interest: a news spike or an anniversary inflates one month, not six. A month
in which a page seen in another month has no dump row counts as zero, so a
page viewed in one month of twelve has a median of zero, not that month's
total; pages seen in no joined month are null.

**Source.** The Wikimedia
[pageview_complete](https://dumps.wikimedia.org/other/pageview_complete/readme.html)
monthly dumps, `pageviews-YYYYMM-user.bz2` (never the `-automated` file, which
is bots). One line per (project, title, page_id, access method), every project
in one ~6 GB file sorted by project.

**Cache.** `scripts/pageviews.py --month YYYY-MM` reads one month and writes
the en.wikipedia rows, summed over access methods per `page_id`, to
`data/pageviews/en/YYYY-MM.parquet` (~65 MB; columns `month`, `page_id`,
`title`, `views`; `title` is set only on the few rows the dump publishes
without a page id, which the build joins on title). The
[`pageviews.yml`](.github/workflows/pageviews.yml) workflow runs it on the
5th of each month and publishes the file to R2 at
`wiki-geoparquet/pageviews/en/YYYY-MM.parquet`, one month per run, skipping a
month already there; a backfill is one `workflow_dispatch` per month. The dump
is sorted by project and bzip2 blocks are independent, so the extractor
binary-searches the file over HTTP ranges for the block where en.wikipedia
starts and reads only that stretch (~0.9 GB of the 6 GB). A stream that ends
inside the en.wikipedia block is an error, not a partial file: nothing is
written, the run fails, and a cached month whose metadata says
`complete=false` is ignored by the build. One month took
3–3.5 minutes on a residential 4–5 MiB/s line, download-bound; a GitHub
runner, with faster network but two cores for the bzip2 decode and the line
scan, is projected at 3–5 minutes.

**Build.** `extract.py` fetches the last N months it lacks from R2 into
`data/pageviews/en/`, joins the ones that exist (page_id first, title
fallback) and records them in the `pageviews_months` metadata. A month nobody
has is a warning, not a failure: the build goes on with fewer months. A
six-hour cap on a build job is why the dumps are read elsewhere.

```sh
make pageviews PV_MONTH=2026-09       # extract one month into the local cache
make pageviews-check                  # coverage + known pages against the output
```

## Feature size from OSM

`extent_km`, `area_km2` and `osm_source` come from OpenStreetMap, joined from
one bulk extract of every Wikipedia-linked OSM way and relation. There is no
geometry column: the bbox numbers keep rows small for the DuckDB-Wasm shards
that read this file.

**Assessment.** Four ways to get a feature's OSM geometry were weighed:

- *Per-feature Overpass, by Wikidata P402 relation id and by the `wikidata=`
  tag.* P402 is on 46 % (927) of the top 2,000 natural-class rows by
  pageviews, and an id lookup is cheap; the tag lookup is an exact-tag query
  over a key with millions of values and Overpass is slow at it. Measured on
  the public instance over about an hour of chained runs: 126 of the 2,000
  candidates answered (107 with an extent, 45 with an area), the rest 504,
  429 and finally a refused connection. Not a path to ~20k natural rows, even
  chained; the measurement is what motivated the bulk extract.
- *A planet extract through `osmium tags-filter wikidata`.* The full answer,
  but a 70+ GB download and a multi-hour filter: not for CI.
- *The QLever osm-planet SPARQL endpoint* (osm2rdf: `osmkey:wikidata`,
  `osmkey:wikipedia`, `geo:asWKT`, `osm2rdf:area`, `geof:envelope`). One
  query family returns every tagged way and relation with its envelope and
  area, ordered by object and paged with LIMIT/OFFSET; the first page carries
  the whole computation (about two minutes) and the rest come from the
  server's cache in seconds. Chosen. The two queries (wikidata-tagged objects; wikipedia-tagged objects
  without a QID) returned 2,987,290 ways and relations in 3.5 minutes on
  2026-10-13, 13 pages of 250,000 rows, a 144 MiB Parquet.

**What is recorded.** For each OSM way or relation: type and id, QID, Wikipedia
language and title (the dump's underscore form), bbox, `extent_km` (the
bbox's longer side) and `area_km2` (osm2rdf's area for closed ways and
multipolygon relations; an open way such as a range drawn as a line has an
extent and no area). A feature mapped only as a node (many ranges and plateaus
are) gets no size, and neither does one whose only OSM object has a
point-sized envelope: an object without a positive extent is not a match, so
the page keeps null size fields. The build joins a page to OSM objects by QID, by `en:` title
and by the P402 relation id Wikidata names for it; of several objects the
largest extent wins. An envelope wider than 180° straddles the antimeridian
and says nothing about the feature's width (Russia, Antarctica and the United
States all come back as −180..180), so such an object is flagged
`antimeridian` in the extract and carries no extent. Measured on the top 2,000 natural-class rows by
pageviews: 1,319 have `extent_km` (66 %) and 1,250 `area_km2` (62 %); over
the whole 2026-10-09 release 534,609 rows (38.8 %) match. The unmatched
are mostly peaks mapped as nodes (Everest, K2, Denali, Fuji) and seas and
ranges with no OSM object (Gulf of Mexico, Appalachian Mountains).

**Cache.** `scripts/osm_extent.py --extract` writes one extract per month,
`data/osm/extent-YYYY-MM.parquet` (R2: `wiki-geoparquet/osm/extent-YYYY-MM.parquet`).
The [`osm-extent.yml`](.github/workflows/osm-extent.yml) workflow runs it on
the 3rd of each month and on dispatch, skipping a month already on R2. The
build fetches the newest extract within six months (`--osm-dir`, `--osm-url`)
and joins it; with none the three columns are null and the build goes on.
Nothing queries OSM from inside the six-hour build job.

```sh
make osm                                   # this month's extract from QLever
make osm-report OSM_SOURCE=data/prev/wikipedia_geotagged.parquet OSM_TOP=2000
```

**Classes.** `instance_of` / `instance_of_qid` already keep a mountain range
apart from a mountain: `Q46831` "mountain range" (3,984 rows), `Q1437459`
"non-geologically related mountain range" (Rocky Mountains), `Q2624046`
"mountain chain", `Q674541` "low mountain range", against `Q8502` "mountain".
Folding ranges into mountains is a consumer's taxonomy choice, not this file's.

## How it works

1. Streams the Wikidata truthy N-Triples dump (~70 GB) to extract P625
   coordinates and enrichment properties (instance type, country, population,
   GeoNames ID, images)
2. Resolves P31/P17 QIDs to human-readable labels via the Wikidata API
3. Streams 5 Wikipedia SQL dump files (~12 GB) for article metadata, images,
   descriptions, and inlink counts
4. Writes Hilbert-sorted GeoParquet with bbox covering via DuckDB spatial
5. Joins the pageviews and OSM extent caches (filled by their own workflows)
6. Pipes DuckDB to [tippecanoe](https://github.com/felt/tippecanoe) for PMTiles
   with attribute-based feature dropping

See the [Makefile](Makefile) for build steps.

## Data source

Coordinates and enrichment from
[Wikidata entity dumps](https://dumps.wikimedia.org/wikidatawiki/entities/),
article metadata from
[English Wikipedia SQL dumps](https://dumps.wikimedia.org/enwiki/latest/).
Both released under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
Pageviews from the
[pageview_complete](https://dumps.wikimedia.org/other/pageview_complete/) dumps
(CC0). Feature sizes from [OpenStreetMap](https://www.openstreetmap.org/copyright)
via Overpass, © OpenStreetMap contributors, [ODbL](https://opendatacommons.org/licenses/odbl/).

## License

Code: [MIT](LICENSE) Data outputs:
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (derived from
Wikipedia/Wikidata)
