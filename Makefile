.PHONY: download extract tiles validate build release clean

DUMP_BASE := https://dumps.wikimedia.org/enwiki/latest
DUMP_DIR  := data/dumps
DUMPS     := geo_tags page page_props linktarget pagelinks

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
	TIPPECANOE=$(HOME)/dev/carbonplan/tippecanoe/tippecanoe uv run python scripts/build_pmtiles.py

validate:
	uv run python scripts/validate.py

build: extract tiles validate
	@echo "Build complete."

release:
	@TAG=$$(date +v%Y-%m); \
	echo "Creating release $$TAG..."; \
	gh release create $$TAG \
		data/wikipedia_geotagged.parquet \
		data/wikipedia_geotagged.pmtiles \
		--title "Wikipedia Geo $$TAG" \
		--notes "Monthly rebuild from Wikipedia dumps."

clean:
	rm -f data/*.parquet data/*.pmtiles
