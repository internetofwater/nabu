# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from pathlib import Path
from rdflib import Graph
import requests
import csv
from lib import validate_jsonld


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
    print(f"Fetching {config.oaf_items_endpoint}")
    collection_resp = requests.get(
        config.oaf_items_endpoint, headers={"Accept": "application/json"}
    )
    collection_resp.raise_for_status()
    collection = collection_resp.json()

    csv_header_row = ["id", "target", "creator", "description"]
    csv_rows = []

    if config.check_shacl:
        print(f"Validating against {config.shacl_shape}")
    else:
        print("Skipping SHACL validation")

    for feature in collection["features"]:
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
            
            print(f"SHACL Validation passed for {jsonld_url}")

        csv_rows.append(
            [f"https://geoconnex.us/{config.geoconnex_namespace}/{feature_id}", feature_url, config.contact_email, config.description]
        )

    assert config.output_path, "output_path must be set"
    output_path = Path(config.output_path).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path.resolve().absolute(), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_header_row)
        writer.writerows(csv_rows)

        if config.print_to_stdout:
            print(csv_header_row)
            print(csv_rows)
            
    print(f"CSV written to {output_path.absolute()}")
