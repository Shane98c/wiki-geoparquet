.PHONY: download extract tiles search validate build release upload clean

DUMP_BASE   := https://dumps.wikimedia.org/enwiki/latest
DUMP_DIR    := data/dumps
DUMPS       := geo_tags page page_props linktarget pagelinks
PARQUET     := data/wikipedia_geotagged.parquet
PMTILES     := data/wikipedia_geotagged.pmtiles
SEARCH      := data/wikipedia_search.parquet
TIPPECANOE  ?= tippecanoe

download:
	mkdir -p $(DUMP_DIR)
	@pids=""; fail=0; i=0; \
	for f in $(DUMPS); do \
		echo "Downloading enwiki-latest-$$f.sql.gz..."; \
		curl -fsSL --retry 5 --retry-delay 5 -C - -o $(DUMP_DIR)/enwiki-latest-$$f.sql.gz \
			$(DUMP_BASE)/enwiki-latest-$$f.sql.gz & \
		pids="$$pids $$!"; \
		i=$$((i+1)); if [ $$i -lt 5 ]; then sleep 2; fi; \
	done; \
	for pid in $$pids; do \
		wait $$pid || fail=1; \
	done; \
	if [ $$fail -ne 0 ]; then echo "ERROR: one or more downloads failed"; exit 1; fi
	@echo "All downloads complete."

extract: $(PARQUET)
$(PARQUET): scripts/extract.py
	uv run python scripts/extract.py

tiles: $(PARQUET)
	duckdb -c " \
		LOAD spatial; \
		COPY ( \
			SELECT \
				'Feature' AS type, \
				ST_AsGeoJSON(geometry)::JSON AS geometry, \
				json_object( \
					'page_id', page_id, \
					'qid', qid, \
					'label', label, \
					'description', description, \
					'gt_type', gt_type, \
					'page_len', page_len, \
					'inlink_count', inlink_count, \
					'gt_primary', gt_primary, \
					'wikipedia_url', wikipedia_url, \
					'image_url', image_url \
				) AS properties \
			FROM '$(PARQUET)' \
		) TO STDOUT (FORMAT json, ARRAY false); \
	" | $(TIPPECANOE) \
		-o $(PMTILES) -f -l wikipedia \
		--name="Wikipedia Geotagged Articles" \
		--attribution="Wikipedia/Wikidata, CC BY-SA 4.0" \
		--minimum-zoom=0 -zg \
		--drop-by-attribute-as-needed=page_len \
		--extend-zooms-if-still-dropping \
		-r1

validate: $(PARQUET)
	uv run python scripts/validate.py

search: $(PARQUET)
	duckdb -c " \
		LOAD spatial; \
		COPY ( \
			SELECT label, bbox.xmin AS lon, bbox.ymin AS lat \
			FROM '$(PARQUET)' \
			ORDER BY label \
		) TO '$(SEARCH)' (FORMAT PARQUET, COMPRESSION ZSTD); \
	"

build: extract tiles search validate
	@echo "Build complete."

release:
	@TAG=$$(date +v%Y-%m-%d); \
	echo "Creating release $$TAG..."; \
	gh release view $$TAG >/dev/null 2>&1 || \
		gh release create $$TAG \
			--target $$(git rev-parse HEAD) \
			--title "Wikipedia Geo $$TAG" \
			--notes "Monthly rebuild from Wikipedia dumps."; \
	gh release upload $$TAG \
		$(PARQUET) \
		$(PMTILES) \
		--clobber

upload:
	wrangler r2 object put wiki-geoparquet/v1/wikipedia_geotagged.parquet \
		--file $(PARQUET) --content-type application/vnd.apache.parquet --remote
	wrangler r2 object put wiki-geoparquet/v1/wikipedia_geotagged.pmtiles \
		--file $(PMTILES) --content-type application/vnd.pmtiles --remote
	wrangler r2 object put wiki-geoparquet/v1/wikipedia_search.parquet \
		--file $(SEARCH) --content-type application/vnd.apache.parquet --remote

clean:
	rm -f data/*.parquet data/*.pmtiles
