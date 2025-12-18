# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = ["geopandas"]
# ///

import geopandas as gpd

# This is script for creating a flatgeobuf which can be used for
# testing on a small subset of data.
# https://features.geoconnex.dev/collections/dams/items/1000403?f=json

url = "https://storage.googleapis.com/national-hydrologic-geospatial-fabric-reference-hydrofabric/reference_catchments_and_flowlines.fgb"
gdf = gpd.read_file(
    url,
    engine="fiona",
    bbox=[
        -108,
        37,
        -107.5,
        37.4,
    ],
)

print(gdf)

gdf.to_file("colorado_subset.fgb", driver="FlatGeobuf")
