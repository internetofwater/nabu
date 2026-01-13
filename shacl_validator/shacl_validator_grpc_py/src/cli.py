# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
from pathlib import Path

from rdflib import Graph
from geoconnex_gen import GeoconnexCSVConfig, generate_geoconnex_csv

# Import validation logic
from lib import check_jsonld_from_oaf_endpoint, validate_jsonld_from_url
from server import serve

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for the server."""
    parser = argparse.ArgumentParser(description="SHACL Validation gRPC Server")

    subparsers = parser.add_subparsers(dest="command", required=True)

    server_subparser = subparsers.add_parser("serve", help="Start the gRPC server for SHACL validation")
    server_subparser.add_argument(
        "--grpc_port",
        type=int,
        default="50051",
        help="grpc port to listen on",
    )
    server_subparser.add_argument(
        "--http_port",
        type=int,
        default="8000",
        help="http port to listen on",
    )
    server_subparser.add_argument(
        "--shacl_file",
        type=str,
        default=str(
            (
                Path(__file__).parent.parent.parent / "shapes" / "geoconnex.ttl"
            ).absolute()
        ),
        help="Path to the shacl file to use for validation",
    )


    check_oaf_subparser = subparsers.add_parser("check_oaf", help="Check jsonld from an OGC API-Features endpoint")
    check_oaf_subparser.add_argument(
        "--endpoint", type=str, help="OGC API-Features endpoint"
    )
    check_oaf_subparser.add_argument(
        "--collection", type=str, help="OGC API-Features collection"
    )

    check_url_subparser = subparsers.add_parser("check_url", help="Check jsonld from a single url")
    check_url_subparser.add_argument("--url", type=str, help="URL to check", required=True)
    check_url_subparser.add_argument("--watch", action="store_true", help="Loop checking the url", default=False)

    generate_geoconnex_csv_subparser = subparsers.add_parser("generate_geoconnex_csv", help="Generate geoconnex csv from a collection")
    generate_geoconnex_csv_subparser.add_argument(
        "--oaf_items_endpoint", type=str, help="The full url to your OGC API-Features collection's items endpoint. Example: https://example.com/api/collections/my_water_data/items", required=True
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--validate_shacl", help="Validate all jsonld items before generating csv", action="store_true", default=False
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--shacl_file", type=str, help="Path to the shacl file to use for validation", required=False, default=str((Path(__file__).parent.parent.parent / "shapes" / "geoconnex.ttl").absolute())
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--description", type=str, help="Description for the geoconnex csv", default="", required=True 
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--contact_email", type=str, help="Contact email for the csv submissions", required=True, default=""
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--geoconnex_namespace", type=str, help="Namespace for the geoconnex csv. Example: wwdh/usace", required=True
    )
    generate_geoconnex_csv_subparser.add_argument(
        "--output_path", type=str, help="Where to save the geoconnex csv", default="~/geoconnex.csv"
    )

    args = parser.parse_args()
    graph = Graph().parse(args.shacl_file)

    if args.command == "check_oaf":
        check_jsonld_from_oaf_endpoint(args.endpoint, args.collection, graph)
    elif args.command == "check_url":
        validate_jsonld_from_url(args.url, watch=args.watch, shacl_shape=graph)
    elif args.command == "generate_geoconnex_csv":
        generate_geoconnex_csv(
            GeoconnexCSVConfig(
                oaf_items_endpoint=args.oaf_items_endpoint,
                description=args.description,
                contact_email=args.contact_email,
                shacl_shape=args.shacl_file,
                check_shacl=args.validate_shacl,
                geoconnex_namespace=args.geoconnex_namespace,
                output_path=args.output_path
            )
        )
    else:
        logger.info(f"Starting SHACL Validation grpc Server on {args.grpc_port}")
        logger.info(f"SHACL file used for validation: {args.shacl_file}")
        serve(graph, grpc_port=args.grpc_port, http_port=args.http_port)


if __name__ == "__main__":
    main()
