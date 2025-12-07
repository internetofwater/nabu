# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import duckdb
from starlette.responses import JSONResponse
from starlette.requests import Request

logger = logging.getLogger("uvicorn.error")


CATCHMENTS_FILE = os.environ.get(
    "CATCHMENTS_FILE",
    "gcs://national-hydrologic-geospatial-fabric-reference-hydrofabric/reference_catchments_and_flowlines.fgb",
)


async def initialize_duckdb():
    if (
        not os.path.exists(CATCHMENTS_FILE)
        and not CATCHMENTS_FILE.startswith("gcs://")
        and not CATCHMENTS_FILE.startswith("s3://")
    ):
        logger.warning(
            f"Catchments file not found at {CATCHMENTS_FILE}; skipping duckdb initialization"
        )
        return

    logger.info("Creating DuckDB connection")
    global con
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")


async def get_mainstem(request: Request):
    """Given a point, return the Geoconnex mainstem associated with it"""
    isPoint = "point" in request.query_params
    isBbox = "bbox" in request.query_params

    minx: float
    miny: float
    maxx: float
    maxy: float

    match isPoint, isBbox:
        case True, True:
            return JSONResponse(
                {
                    "error": "You cannot specify both a point and a bounding box to filter by"
                },
                status_code=400,
            )
        case False, False:
            return JSONResponse(
                {
                    "error": "You must specify either a point or a bounding box to filter by"
                },
                status_code=400,
            )
        case True, False:
            points = request.query_params["point"].split(",")
            if len(points) != 2:
                return JSONResponse(
                    {"error": "Point must be specified as [longitude, latitude]"},
                    status_code=400,
                )
            minx = float(points[0])
            miny = float(points[1])
            maxx = minx
            maxy = miny
        case False, True:
            points = request.query_params["bbox"].split(",")
            if len(points) != 4:
                return JSONResponse(
                    {"error": "Bbox must be specified as [minx, miny, maxx, maxy]"},
                    status_code=400,
                )
            minx = float(points[0])
            miny = float(points[1])
            maxx = float(points[2])
            maxy = float(points[3])

    # Query for the catchment containing the point
    query = f"""SELECT *
    FROM ST_Read(
        '{CATCHMENTS_FILE}',
        spatial_filter_box = ST_MakeBox2D(
            ST_Point({minx}, {miny}),
            ST_Point({maxx}, {maxy})
        )
    )"""

    df= con.execute(query).df()
    df["geom"] = df["geom"].astype(str)

    json_data = df.to_json(orient="records", default_handler=str)
    return JSONResponse(content=json.loads(json_data))
