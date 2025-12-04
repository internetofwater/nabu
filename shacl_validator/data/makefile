ORAS_CACHE := $(CURDIR)/.oras/cache
export ORAS_CACHE

# To run this you must get the gpkg files which be downloaded from
# https://www.sciencebase.gov/catalog/item/61295190d34e40dd9c06bcd7
generate_mainstem_gpkg:
	# Create a new merged GeoPackage with the catchments layer, reprojected to EPSG:4326
	ogr2ogr -t_srs EPSG:4326 merged.gpkg reference_catchments.gpkg

	# Append the flowlines layer, reprojected to EPSG:4326
	ogr2ogr -t_srs EPSG:4326 -update -nln flowlines merged.gpkg reference_flowline.gpkg


pull_mainstem_gpkg:
	oras pull ghcr.io/internetofwater/geoconnex-mainstem-lookup-gpkg:latest

backup_mainstem_gpkg:
	@echo -n "Enter your GitHub token: "; \
	read GITHUB_TOKEN; \
	oras push ghcr.io/internetofwater/geoconnex-mainstem-lookup-gpkg:latest merged.gpkg:application/octet-stream \
		--username internetofwater \
		--password "$$GITHUB_TOKEN" \
		--annotation 'org.opencontainers.image.source=https://github.com/internetofwater/nabu'
		--annotation 'org.opencontainers.image.description=The GeoPackage containing all the data from the Reference Hydrofabric here: https://www.sciencebase.gov/catalog/item/61295190d34e40dd9c06bcd7'
