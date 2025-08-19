# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Literal, assert_never
import pyshacl
from rdflib import Graph, RDF, URIRef

location_oriented_path = Path(__file__).parent.parent / "shacl_shapes" / "locationOriented.ttl"
dataset_oriented_path = Path(__file__).parent.parent / "shacl_shapes" / "datasetOriented.ttl"
SCHEMA = "https://schema.org/"

location_oriented_shacl = Graph().parse(location_oriented_path, format="turtle")
dataset_oriented_shacl = Graph().parse(dataset_oriented_path, format="turtle")

def validate_jsonld(jsonld: str, format: Literal["location_oriented", "dataset_oriented"] ):
    data_graph = Graph()
    data_graph.parse(data=jsonld, format="json-ld")

    match format:
        case "location_oriented":


            place_iri = URIRef(SCHEMA + "Place")

            if not any(data_graph.subjects(RDF.type, place_iri)):
                return False, "", "SHACL Validation failed: Location Oriented jsonld must have '@type': 'Place'"

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

