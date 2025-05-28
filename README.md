# Nabu 

[![codecov](https://codecov.io/gh/internetofwater/nabu/branch/master/graph/badge.svg?token=KtA15glWkf)](https://codecov.io/gh/internetofwater/nabu) 
[![goreportcard status](https://goreportcard.com/badge/github.com/internetofwater/nabu)](https://goreportcard.com/report/github.com/internetofwater/nabu)

Nabu is a CLI program for:
- crawling remote JSON-LD documents from a remote sitemap and storing them into an S3 bucket
- synchronizing JSON-LD documents from the S3 bucket into a triplestore by:
    1. removing old triples from the triplestore that are no longer present in S3
    2. inserting triples into the triplestore which are present in S3 but not in the triplestore

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

## Technical Details

For technical details and design rationale see the [docs](docs/) folder. 

## Fork Details

This repo is a heavily modified fork of the gleanerio [Nabu](https://github.com/gleanerio/nabu) repo: 

It was edited to:
- add unit tests, integration tests, code coverage, and linting to the entire project
- add crawl support from [Gleaner](https://github.com/gleanerio/gleaner) into the Nabu binary
- add shacl validation
- refactor code to make it easier to maintain and extend
- improve concurrency
- add observability and tracing output using OpenTelemetry