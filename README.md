# Nabu 

[![codecov](https://codecov.io/gh/internetofwater/nabu/branch/master/graph/badge.svg?token=KtA15glWkf)](https://codecov.io/gh/internetofwater/nabu) 
[![goreportcard status](https://goreportcard.com/badge/github.com/internetofwater/nabu)](https://goreportcard.com/report/github.com/internetofwater/nabu)

Nabu is the central data engineering tool for [geoconnex](https://docs.geoconnex.us/). It is a CLI for
- crawling remote JSON-LD documents from a remote sitemap and storing them into an S3 bucket
- preparing data for ingestion into a graph database by:
    - validating RDF data against [SHACL](https://en.wikipedia.org/wiki/SHACL) shapes
    - converting JSON-LD documents from the S3 bucket into N-Quad files
    - enriching N-Quads with additional hydrologic metadata such as mainstem identifiers

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