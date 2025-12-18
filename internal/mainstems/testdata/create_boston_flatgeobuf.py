# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = ["geopandas"]
# ///

import geopandas as gpd

# Boston bounding box (minx, miny, maxx, maxy)
boston_bbox = (-71.1912, 42.2279, -70.9860, 42.3995)

# Load a shapefile or any other supported vector format, filtered to Boston
gdf = gpd.read_file(
    "https://storage.googleapis.com/national-hydrologic-geospatial-fabric-reference-hydrofabric/reference_catchments_and_flowlines.fgb",
    bbox=boston_bbox,
)

# Save the subset as FlatGeobuf
gdf.to_file("boston_catchments.fgb", driver="FlatGeobuf")
