.PHONY: download extract tiles validate build release clean

DUMP_BASE   := https://dumps.wikimedia.org/enwiki/latest
DUMP_DIR    := data/dumps
DUMPS       := geo_tags page page_props linktarget pagelinks
PARQUET     := data/wikipedia_geotagged.parquet
PMTILES     := data/wikipedia_geotagged.pmtiles
TIPPECANOE  ?= tippecanoe

download:
	mkdir -p $(DUMP_DIR)
	@for f in $(DUMPS); do \
		echo "Downloading enwiki-latest-$$f.sql.gz..."; \
		curl -L --retry 3 -C - -o $(DUMP_DIR)/enwiki-latest-$$f.sql.gz \
			$(DUMP_BASE)/enwiki-latest-$$f.sql.gz; \
	done

extract:
	uv run python scripts/extract.py

tiles:
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

validate:
	uv run python scripts/validate.py

build: extract tiles validate
	@echo "Build complete."

release:
	@TAG=$$(date +v%Y-%m); \
	echo "Creating release $$TAG..."; \
	gh release create $$TAG \
		$(PARQUET) \
		$(PMTILES) \
		--title "Wikipedia Geo $$TAG" \
		--notes "Monthly rebuild from Wikipedia dumps."

clean:
	rm -f data/*.parquet data/*.pmtiles
