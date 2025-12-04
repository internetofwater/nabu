# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from pathlib import Path
import duckdb
import pandas as pd

from starlette.responses import JSONResponse
from starlette.requests import Request

logger = logging.getLogger("uvicorn.error")



async def initialize_duckdb():
    GPKG_FILE = os.environ.get("MAINSTEM_GPKG_FILE", str(Path(__file__).parent.parent.parent / "data" / "merged.gpkg"))

    if not os.path.exists(GPKG_FILE):
        logger.warning(f"GPKG file not found at {GPKG_FILE}; skipping duckdb initialization")
        return

    logger.info("Creating DuckDB connection")
    global con
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    logger.info("Loading catchments into DuckDB")
    con.execute(f"""
    CREATE TABLE catchments AS 
    SELECT * FROM st_read('{GPKG_FILE}', layer='reference_catchments')
    """)
    logger.info("Creating spatial index")
    con.execute("""
    CREATE INDEX catchments_geom_idx ON catchments USING rtree(geom);
    """)
    logger.info("Loading flowlines into DuckDB")
    con.execute(f"""
    CREATE TABLE flowlines AS 
    SELECT * FROM st_read('{GPKG_FILE}', layer='flowlines')
    """)

    mainstem_lookup = pd.read_csv(
        "https://github.com/internetofwater/ref_rivers/releases/download/v2.1/mainstem_lookup.csv"
    )
    mainstem_lookup["lp_mainstem"] = mainstem_lookup["lp_mainstem"].astype(int)
    mainstem_lookup["ref_mainstem_id"] = mainstem_lookup["ref_mainstem_id"].astype(int)
    con.register("mainstem_lookup", mainstem_lookup)

async def get_mainstem(request: Request):
    """Given a point, return the Geoconnex mainstem associated with it"""
    if "lon" not in request.query_params or "lat" not in request.query_params:
        return JSONResponse(
            {"error": "Missing 'lon'/'lat' query parameters"}, status_code=400
    )

    try:
        lon = float(request.query_params["lon"])
        lat = float(request.query_params["lat"])
    except (ValueError, TypeError):
        return JSONResponse(
            {"error": "Invalid 'lon'/'lat' query parameters"},
            status_code=400,
        )

    # Query for the catchment containing the point
    catchment_query = f"""
    SELECT featureid 
    FROM catchments
    WHERE ST_Intersects(geom, ST_Point({lon}, {lat}))
    LIMIT 1
    """
    catchment_result = con.execute(catchment_query).fetchone()
    if not catchment_result:
        return JSONResponse(
            {"error": "No catchment found for this point"}, status_code=404
        )

    feature_id = int(catchment_result[0])

    # Query flowline for that catchment
    flowline_query = f"""
    SELECT "TerminalPa" AS terminal_path
    FROM flowlines
    WHERE COMID = {feature_id}
    LIMIT 1
    """
    flowline_result = con.execute(flowline_query).fetchone()
    if not flowline_result:
        return JSONResponse(
            {"error": "No flowline found for this catchment"}, status_code=404
        )

    terminal_path_id = int(flowline_result[0])

    # Lookup mainstem
    mainstem_query = f"""
    SELECT ref_mainstem_id FROM mainstem_lookup
    WHERE lp_mainstem = {terminal_path_id}
    LIMIT 1
    """
    mainstem_result = con.execute(mainstem_query).fetchone()
    if not mainstem_result:
        return JSONResponse(
            {"error": "No Geoconnex mainstem found for this flowline"}, status_code=404
        )

    mainstem_id = int(mainstem_result[0])
    mainstem_url = (
        f"https://reference.geoconnex.us/collections/mainstems/items/{mainstem_id}"
    )

    return JSONResponse(
        {
            "reference_mainstem_id": mainstem_id,
            "mainstem_url": mainstem_url,
            "catchment_id": feature_id,
            "terminal_flowline_id": terminal_path_id,
        }
    )
