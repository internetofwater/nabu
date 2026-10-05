# Nabu 

[![codecov](https://codecov.io/gh/internetofwater/nabu/branch/master/graph/badge.svg?token=KtA15glWkf)](https://codecov.io/gh/internetofwater/nabu) 
[![goreportcard status](https://goreportcard.com/badge/github.com/internetofwater/nabu)](https://goreportcard.com/report/github.com/internetofwater/nabu)

Nabu is the central data engineering tool for [geoconnex](https://docs.geoconnex.us/). It is a CLI for
- crawling remote JSON-LD documents from a remote sitemap and storing them in an S3 bucket as one GeoParquet file per sitemap
    - each row contains the `@id`, `feature_name`, `feature_description`, WKB `geometry`, S2 cell id of the geometry, and the `jsonld` of a document
    - enriching documents with additional hydrologic metadata, such as adding the associated mainstem to the JSON-LD and the `mainstem_uri` column
- preparing data for ingestion into a graph database by:
    - validating RDF data against [SHACL](https://en.wikipedia.org/wiki/SHACL) shapes
    - streaming the JSON-LD documents in the parquet files to stdout as N-Quads with `nabu nquads`

For more technical details see the [docs](docs/) folder.

See the [examples](examples/) directory for example CLI usage.

# Installation

## Docker

```sh
docker run internetofwater/nabu:latest
```

## Native Binary

```sh
git clone https://github.com/internetofwater/nabu
cd nabu
go build ./cmd/nabu
./nabu --help
```

## Fork Details

This repo is a completely rewritten fork of the gleanerio [Nabu](https://github.com/gleanerio/nabu) repo