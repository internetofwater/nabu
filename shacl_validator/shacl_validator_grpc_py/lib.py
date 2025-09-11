# SPDX-License-Identifier: Apache-2.0

import logging
from time import sleep
import pyshacl
from rdflib import Graph, RDF, URIRef
import requests

SCHEMA = "https://schema.org/"


def validate_graph(data_graph: Graph, shacl_shape: Graph):
        place_iri = URIRef(SCHEMA + "Place")
        dataset_iri = URIRef(SCHEMA + "Dataset")

        if not any(data_graph.subjects(RDF.type, place_iri)) and not any(data_graph.subjects(RDF.type, dataset_iri)):
            return (
                False,
                "",
                "SHACL Validation failed: the top level node of the jsonld must have '@type': 'schema:Place' or '@type': 'schema:Dataset'",
            )

        return pyshacl.validate(
            data_graph,
            inference="none",
            shacl_graph=shacl_shape,
            data_graph_format="json-ld",  # Explicitly state data graph format
            shacl_graph_format="turtle",  # Explicitly state shapes graph format
        )

def validate_jsonld_from_url(url: str, shacl_shape: Graph, watch: bool):
    lastPrint = ""
    try:
        while True:
            response = requests.get(url)
            try:
                response.raise_for_status()
                jsonld = response.json()
            except Exception as text:
                if lastPrint != str(text):
                    logging.error(f"{text}")
                    print(response.text, flush=True, end="\n\n")
                lastPrint = str(text)
                continue
            conforms, _, text = validate_jsonld(jsonld, shacl_shape)
            if not conforms:
                if text != lastPrint:
                    logging.info(f"{text}\n\n")
                lastPrint = text
            else:
                text = "Shacl Validation passed"
                if lastPrint != text:
                    logging.info("Shacl Validation passed\n\n")
                lastPrint = text
            if not watch:
                return
            sleep(3)
    except KeyboardInterrupt:
        pass

def validate_jsonld(jsonld: str, shacl_shape: Graph):
    data_graph = Graph()
    data_graph.parse(data=jsonld, format="json-ld")
    return validate_graph(data_graph, shacl_shape)

   
def check_jsonld_from_oaf_endpoint(endpoint: str , collection_to_check: str, shacl_shape: Graph):

    logging.info(f"Checking {endpoint} for {collection_to_check}")

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
        conforms, _, text = validate_jsonld(jsonld, shacl_shape=shacl_shape)
        
        if not conforms:
            print(f"SHACL Validation failed for {id}: \n{text}")