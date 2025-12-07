ORAS_CACHE := $(CURDIR)/.oras/cache
export ORAS_CACHE

# To run this you must have the gpkg files which be downloaded from
# https://www.sciencebase.gov/catalog/item/61295190d34e40dd9c06bcd7
generate_flatgeobuf:
	uv run join.py

pull_mainstem_flatgeobuf:
	oras pull ghcr.io/internetofwater/reference_catchments_and_flowslines:latest

backup_mainstem_flatgeobuf:
	@echo -n "Enter your GitHub token: "; \
	read GITHUB_TOKEN; \
	oras push ghcr.io/internetofwater/reference_catchments_and_flowslines:latest reference_catchments_and_flowslines.fgb:application/octet-stream \
		--username internetofwater \
		--password "$$GITHUB_TOKEN" \
		--annotation 'org.opencontainers.image.source=https://github.com/internetofwater/nabu' \
		--annotation 'org.opencontainers.image.description=The flatgeobuf files containing all the data from the Reference Hydrofabric here: https://www.sciencebase.gov/catalog/item/61295190d34e40dd9c06bcd7'
