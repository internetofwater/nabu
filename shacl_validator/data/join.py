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

import geopandas as gpd
import pandas as pd

catchments = gpd.read_file(
    "reference_catchments.gpkg"
)
catchments = catchments.to_crs(4326)
# We add a prefix so that the column names are unique
# and easier to understand after the join
catchments = catchments.add_prefix("Catchment_")

flowlines = gpd.read_file("reference_flowline.gpkg")
flowlines = flowlines.to_crs(4326)
flowlines = flowlines.add_prefix("Flowline_")

catchments_with_associated_flowline = catchments.merge(
    flowlines,
    left_on="Catchment_featureid",
    right_on="Flowline_COMID",
    how="left",
)

# The mainstem lookup file is from the internet of water
# csv release; it needs to be cast to an integer 
# for the join since the original type is a string
mainstem_lookup = pd.read_csv(
    "https://github.com/internetofwater/ref_rivers/releases/download/v2.1/mainstem_lookup.csv",
    dtype={"lp_mainstem": "Int64", "ref_mainstem_id": "Int64"}
)
mainstem_lookup = mainstem_lookup.add_prefix("Mainstem_Metadata_")

final = catchments_with_associated_flowline.merge(
    mainstem_lookup,
    # LevelPathI == The LevelPath identifier. This groups together a set of flowlines that form a single mainstem path.
    # All lines with the same LevelPathI form one continuous routed path (e.g., the entire main stem of a river).
    left_on="Flowline_LevelPathI",
    right_on="Mainstem_Metadata_lp_mainstem",
    how="left",
)

final["geoconnex_url"] = final["Mainstem_Metadata_ref_mainstem_id"].apply(
    lambda x: (
        f"https://reference.geoconnex.us/collections/mainstems/items/{int(x)}"
        if pd.notnull(x)
        else None
    )
)

# We  drop the flowline geometry since it isn't needed in the final
# result and would make our file larger and have a second geometry
# column; flatgeobuf can't index on multiple geometry columns
# so we only keep the catchment geometry that we actually need for the lookup;
# The mainstem geometry can be retrieved from the geoconnex reference
# server
final = final.drop(columns="Flowline_geometry")
final.to_file("reference_catchments_and_flowlines.fgb")
