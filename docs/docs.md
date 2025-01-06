## Repo Layout

- `pkg` defines the cli and config readers that are public to the client
- `internal` defines functions that are not directly user facing in the cli
    - `/synchronizer` defines the top level synchronizer client that performs operations with both a triplestore and s3
        - `/graph` defines the operations on _just_ the triple store 
        - `objects` defines the operations on _just_ the s3
    - `/common` defines common helper functions across the repo
     - `/jsonld` defines operations on jsonld and rdf data

## Changes in Refactor

The repo was refactored to:
- separate graph and s3 operations into separate packages. 
- label functions with longer names that would be obvious to someone not familiar with the upstream project