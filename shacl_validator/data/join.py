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
catchment, flowline, and mainstem.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

reference_catchments = Path(__file__).parent / "reference_catchments.gpkg"
assert reference_catchments.exists(), "reference_catchments.gpkg not found; you must download it from sciencebase"

reference_flowline = Path(__file__).parent / "reference_flowline.gpkg"
assert reference_flowline.exists(), "reference_flowline.gpkg not found; you must download it from sciencebase"

print("Reading reference catchments and flowlines")
catchments = gpd.read_file(reference_catchments)
catchments = catchments.to_crs(4326)
# We add a prefix so that the column names are unique
# and easier to understand after the join
catchments = catchments.add_prefix("Catchment_")

flowlines = gpd.read_file(reference_flowline)
flowlines = flowlines.to_crs(4326)
flowlines = flowlines.add_prefix("Flowline_")

print("Joining catchments and flowlines")
catchments_with_associated_flowline = catchments.merge(
    flowlines,
    left_on="Catchment_featureid",
    right_on="Flowline_COMID",
    how="left",
)

print("Loading mainstem lookup csv file")
# The mainstem lookup file is from the internet of water
# csv release; it needs to be cast to an integer 
# for the join since the original type is a string
mainstem_lookup = pd.read_csv(
    "https://github.com/internetofwater/ref_rivers/releases/download/V3/lpv3_lookup.csv",
    dtype={"lp_mainstem_v3": "Int64", "uri": "string"},
)
mainstem_lookup = mainstem_lookup.add_prefix("Mainstem_Metadata_")

print("Joining catchments, flowlines, and mainstem lookup")
final = catchments_with_associated_flowline.merge(
    mainstem_lookup,
    # LevelPathI == The LevelPath identifier. This groups together a set of flowlines that form a single mainstem path.
    # All lines with the same LevelPathI form one continuous routed path (e.g., the entire main stem of a river).
    left_on="Flowline_LevelPathI",
    right_on="Mainstem_Metadata_lp_mainstem_v3",
    how="left",
).rename(columns={"Mainstem_Metadata_uri": "geoconnex_url"})

# We  drop the flowline geometry since it isn't needed in the final
# result and would make our file larger and have a second geometry
# column; flatgeobuf can't index on multiple geometry columns
# so we only keep the catchment geometry that we actually need for the lookup;
# The mainstem geometry can be retrieved from the geoconnex reference
# server
final = final.drop(columns="Flowline_geometry")
final.to_file("reference_catchments_and_flowlines.fgb")

# Ensure that the final result has a few of the expected columns from each of the data sources
# we don't check every column but it must have at least these so its a good enough check
assert "geoconnex_url" in final.columns
assert "Flowline_COMID" in final.columns
assert "Catchment_featureid" in final.columns

percent_catchments_without_geoconnex_mainstem_url = final["geoconnex_url"].isna().mean() * 100

print(f"Percentage of catchments without geoconnex mainstem url: {percent_catchments_without_geoconnex_mainstem_url}")

print("Finished successfully")