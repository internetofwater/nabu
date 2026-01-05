# Nabu Overview

Nabu is the central crawl and data engineering tool for Geoconnex.

The following operations are performed by Nabu. Most can be traced using open telemetry:

1. Nabu harvests data from all sites in a sitemap into an object store
    - Example sitemap is the following: https://geoconnex.us/sitemap.xml
    - If a site fails on an error code that is non fatal and nabu will retry the http request. After multiple retries if the site still fails, Nabu will record the error and continue.
    - If the remote server provides it, it checks the hash of each document
    - If the hashes are different or the document is new, Nabu downloads
    - After crawling, Nabu validates the data is JSON-LD and validates it using SHACL. Only the first N SHACL validation errors will be stored so logs aren't spammed if every site fails the same way. 
    - Nabu communicates with an external shacl validation service over GRPC since there are no Golang SHACL validation libraries
    - Nabu optionally can delete stale JSON-LD files that were not overwritten or found in the latest crawl. (i.e. files that contain features which were removed from the upstream APIs)
    - At the end of a crawl, Nabu puts a crawl report JSON file into the object store. This is used as the data source for the [crawl status page](../crawl-status-page/) so we don't need to add additional cloud infrastructure (i.e. a SQL db)

2. Nabu releases groups of JSON-LD files as one largompressed N-Quad file
    - Nabu converts all JSON-LD files in a given sitemap to N-Quads
    - It defaults to gzip compression to reduce RDF data size
    - Nabu deterministically skolemizes blank nodes in RDF so each triple has a unique stable identifier for all terms. 
    - Nabu generates an associated `.bytesum` hash filee c: this is since conversion depends on streaming from S3 which doesn't guarantee order. Thus it is most efficient to simply keep the sum of the bytes in the file which is essentially an order agnostic hash for the entire sitemap
    - Nabu adds mainstem data during the conversion process to N-Quads. Nabu does this only during conversion so none of the hash info from the original JSON-LD is disrupted. 

3. Nabu can pull sitemap N-Quads to disk in preparation for a graph database to ingest them
    - Nabu uses the `.bytesum` hash to check whether or not to pull. If it is the same both locally and remote, the entire sitemap download can be skipped
    - Nabu does not actually load data directly into the graph database since that might not be portable across database implementations

## Previous Architecture (no longer used)

Nabu previously synced with the graph database by crawling all JSON-LD and then doing a diff between all the items in the object store bucket and the live database. This architecture did not scale and has since been deprecated since:
    - diff'ing the state of the object store and the graph is extremely expensive to calculate
    - it required many round trips against with limited use of batch uploads
    - it required running computationally expensive queries against the production database

It is best to do all data cleaning operations using the object store as the source of truth / staging data store and then once the final data is prepared, upload it to the database in one large bulk upload. 