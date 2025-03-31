# Nabu 
Nabu's main goal is to load/synchronize a collection of RDF graphs (for example JSON-LD document) in an 
object store (minio, S3, etc.) into a graph database/triplestore. 

This repository is a fork of the gleanerio Nabu project. This fork refactored and trimmed code to make it easier to run e2e tests for Geoconnex.

## Properties

- Nabu does not try to manage state of inidividual triples inside the triplestore. If there is any potential for conflict, nabu just drops the entire graph and all its associated triples
    - the s3 bucket is used as the source of truth
        - If an item from one crawl is present in the s3 but it disappears in the next crawl, it will not continue to be present in the triplestore after running nabu synchronization
    - Nabu uses the context item (the fourth context term of the triple) to associate each triple with a graph and thus an individual source
    - All information regarding triples and quads is coming from gleaner output.
        - Nabu does not mutate existing s3 objects 
    - It simply checks if a named graph exists and will drop it before inserting triples of the same graph
- Nabu does not use caching and there is no concept of statefulness from one nabu run to the other. 
- Nabu is agnostic to the content contained inside individual jsonld/nq files as long as they are syntactically valid

### Latency

- Nabu has to post very large triple payloads to the triplestore and thus performance is highly dependent on the efficiency of the backend and network connection
## Repo Layout

- `pkg` defines the cli and config readers that are public to the client
- `internal` defines functions that are not directly user facing in the cli
    - `/synchronizer` defines the top level synchronizer client that performs operations with both a triplestore and s3. Each method on this struct defines a top level operation that moves data between the graph and s3 or keeps them in sync in some way.
        - `/graph` defines the operations on _just_ the triple store 
        - `objects` defines the operations on _just_ the s3
    - `/common` defines common helper functions across the repo

## Changes in Refactor

The repo was refactored to:
- separate graph and s3 operations into separate packages according to what was defined [in the layout section](#repo-layout)
- label functions with longer names that would be easier to understand for someone not familiar with the upstream project or RDF
- handle errors more strictly. All err values are returned to the parent.
    - There are no linter errors when runing `golangci-lint`
- add tests extensively 
- the sparql section in the nabu config file was simplified; the user just needs to specify the endpoint and any triplestore-specific features are handled by us in go
