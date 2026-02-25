# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.12"
# dependencies = ["geopandas", "pandas"]
# ///

"""
This file joins together all catchments and
flowlines into a single file in such a way that
each row can be queried to get the associated
catchment and mainstem.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

reference_catchments = Path(__file__).parent / "reference_catchments.gpkg"
assert reference_catchments.exists(), "reference_catchments.gpkg not found; you must download it from sciencebase"

print("Reading reference catchments")
catchments = gpd.read_file(reference_catchments)
catchments = catchments.to_crs(4326)
# We add a prefix so that the column names are unique
# and easier to understand after the join
catchments = catchments.add_prefix("Catchment_")

print("Loading mainstem lookup csv file")
# The mainstem lookup file is from the internet of water
# csv release; it needs to be cast to an integer 
# for the join since the original type is a string
mainstem_lookup = pd.read_csv(
    "https://github.com/internetofwater/ref_rivers/releases/download/V3/nhdpv2_lookup.csv",
    dtype={"comid": "Int64", "uri": "string"},
)
mainstem_lookup = mainstem_lookup.add_prefix("Mainstem_Metadata_")

print("Joining catchments and mainstem lookup file")
final = catchments.merge(
    mainstem_lookup,
    left_on="Catchment_featureid",
    # join the comid column from the csv with the catchments; we don't
    # need to use the nhd plus flowlines at all since the CSV already provides the lookup comid
    right_on="Mainstem_Metadata_comid",
    how="left",
).rename(columns={"Mainstem_Metadata_uri": "geoconnex_url"})

# drop the mainstem comid column since it is the same as the catchment feature id
final.drop(columns=["Mainstem_Metadata_comid"], inplace=True)

# Ensure that the final result has a few of the expected columns from each of the data sources
# we don't check every column but it must have at least these so its a good enough check
assert "geoconnex_url" in final.columns
assert "Catchment_featureid" in final.columns
assert "Mainstem_Metadata_comid" not in final.columns

percent_catchments_without_geoconnex_mainstem_url = final["geoconnex_url"].isna().mean() * 100

print(f"Percentage of catchments without geoconnex mainstem url: {percent_catchments_without_geoconnex_mainstem_url}")

print("Writing file to disk")
final.to_file("reference_catchments_and_flowlines.fgb")
print("Finished successfully")