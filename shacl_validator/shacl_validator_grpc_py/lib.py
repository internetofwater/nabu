# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from time import sleep
from typing import Literal, assert_never
import pyshacl
from rdflib import Graph, RDF, URIRef
import requests

location_oriented_path = Path(__file__).parent.parent / "shapes" / "locationOriented.ttl"
dataset_oriented_path = Path(__file__).parent.parent / "shapes" / "datasetOriented.ttl"
SCHEMA = "https://schema.org/"

location_oriented_shacl = Graph().parse(location_oriented_path, format="turtle")
dataset_oriented_shacl = Graph().parse(dataset_oriented_path, format="turtle")

def validate_graph(data_graph: Graph, format: Literal["location_oriented", "dataset_oriented"]):
    match format:
        case "location_oriented":
            place_iri = URIRef(SCHEMA + "Place")

            if not any(data_graph.subjects(RDF.type, place_iri)):
                return (
                    False,
                    "",
                    "SHACL Validation failed: Location Oriented jsonld must have '@type': 'schema:Place'",
                )

            return pyshacl.validate(
                data_graph,
                shacl_graph=location_oriented_shacl,
                data_graph_format="json-ld",  # Explicitly state data graph format
                shacl_graph_format="turtle",  # Explicitly state shapes graph format
            )
        case "dataset_oriented":
            return pyshacl.validate(
                data_graph,
                shacl_graph=dataset_oriented_shacl,
                data_graph_format="json-ld",  # Explicitly state data graph format
                shacl_graph_format="turtle",  # Explicitly state shapes graph format
            )
        case _:
            assert_never(format)

def validate_jsonld_from_url(url: str, watch: bool):
    lastPrint = ""
    try:
        while True:
            response = requests.get(url)
            response.raise_for_status()
            jsonld = response.json()
            conforms, _, text = validate_jsonld(jsonld, format="location_oriented")
            if not conforms:
                if text != lastPrint:
                    print(text, flush=True)
                lastPrint = text
            else:
                text = "Shacl Validation passed"
                if lastPrint != text:
                    print("Shacl Validation passed", flush=True)
                lastPrint = text
            if not watch:
                return
            sleep(2)
    except KeyboardInterrupt:
        pass

def validate_jsonld(jsonld: str, format: Literal["location_oriented", "dataset_oriented"] ):
    data_graph = Graph()
    data_graph.parse(data=jsonld, format="json-ld")
    return validate_graph(data_graph, format)

   
def check_jsonld_from_oaf_endpoint(endpoint: str , collection_to_check: str):

    print(f"Checking {endpoint} for {collection_to_check}")

    url = f"{endpoint}/collections/{collection_to_check}/items"

    response = requests.get(url)
    response.raise_for_status()

    json = response.json()

    for feature in json["features"]:
        id = feature["id"]
        url = f"{endpoint}/collections/{collection_to_check}/items/{id}?f=jsonld"
        response = requests.get(url)
        response.raise_for_status()
        jsonld = response.json()
        conforms, _, text = validate_jsonld(jsonld, format="location_oriented")
        
        if not conforms:
            print(f"SHACL Validation failed for {id}: \n{text}")