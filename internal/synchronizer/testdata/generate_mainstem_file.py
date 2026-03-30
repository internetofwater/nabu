# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = ["geopandas", "pandas"]
# ///

"""
This script just generates a small flatgeobuf
file that can be used in tests; it doesn't 
need to be regenerated since the goal is to
just be able to join against it
"""


import geopandas as gpd

df = gpd.read_file(
    "https://storage.googleapis.com/national-hydrologic-geospatial-fabric-reference-hydrofabric/reference_catchments_and_flowlines.fgb"
    ,bbox=(
        -108,
        37,
        -107.5,
        37.4,
    ),
)

assert df.crs == "epsg:4326"

df.to_file("colorado_subset.fgb", driver="FlatGeobuf")