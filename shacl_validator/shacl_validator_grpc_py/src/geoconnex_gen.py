# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
import logging
from pathlib import Path
from rdflib import Graph
import requests
import csv
from lib import validate_jsonld

logger = logging.getLogger(__name__)

@dataclass
class GeoconnexCSVConfig:
    contact_email: str
    check_shacl: bool
    oaf_items_endpoint: str
    shacl_shape: Graph
    description: str
    geoconnex_namespace: str
    output_path: str
    print_to_stdout: bool


def generate_geoconnex_csv(config: GeoconnexCSVConfig):
    logger.info(f"Fetching {config.oaf_items_endpoint}")
    collection_resp = requests.get(
        config.oaf_items_endpoint, headers={"Accept": "application/json"}
    )
    collection_resp.raise_for_status()
    collection = collection_resp.json()

    geoconnex_csv_header_row = ["id", "target", "creator", "description"]
    csv_rows = []

    if config.check_shacl:
        logger.info(f"Validating against {config.shacl_shape}")
    else:
        logger.warning("Skipping SHACL validation")

    # sort by the id so the csv is consistent
    for feature in sorted(collection["features"], key=lambda f: f["id"]):
        feature_id = feature["id"]
        feature_url = f"{config.oaf_items_endpoint}/{feature_id}"
        jsonld_url = f"{feature_url}?f=jsonld"

        if config.check_shacl:
            response = requests.get(jsonld_url)
            response.raise_for_status()
            try:
                jsonld = response.json()
            except Exception as text:
                raise Exception(f"Failed to parse jsonld for {jsonld_url}: \n{text}")
            conforms, _, text = validate_jsonld(jsonld, shacl_shape=config.shacl_shape)

            if not conforms:
                raise Exception(f"SHACL Validation failed for {jsonld_url}: \n{text}")
            
            logger.info(f"SHACL Validation passed for {jsonld_url}")

        csv_rows.append(
            [f"https://geoconnex.us/{config.geoconnex_namespace}/{feature_id}", feature_url, config.contact_email, config.description]
        )

    assert config.output_path, "output_path must be set"
    output_path = Path(config.output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path.resolve().absolute(), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(geoconnex_csv_header_row)
        writer.writerows(csv_rows)

        if config.print_to_stdout:
            with open(output_path.resolve().absolute(), "r", encoding="utf-8") as f:
                print(f.read())

    logger.info(f"CSV written to {output_path.absolute()}")
