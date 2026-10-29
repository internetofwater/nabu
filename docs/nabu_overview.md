# Nabu Overview

Nabu is the central crawl and data engineering tool for Geoconnex.

The following operations are performed by Nabu. Most can be traced using open telemetry:

1. Nabu harvests data from all sites in a sitemap into two parquet files per sitemap in an object store
    - Example sitemap is the following: https://geoconnex.us/sitemap.xml
    - Each sitemap is stored in two files with one row per JSON-LD document, written in the same order. Both are spooled locally while the sitemap is crawled and only uploaded once the harvest succeeds, instead of uploading each JSON-LD document separately
    - `summoned/structured/<sitemap_id>.parquet` is a GeoParquet file with fields extracted from each document, so it can be used as a table without parsing the JSON-LD:
        - `@id`: the `@id` of the document; this is the canonical identifier of the feature
        - `feature_name`: the value of `schema:name`
        - `feature_description`: the value of `schema:description`
        - `geometry`: the `gsp:hasGeometry`/`gsp:asWKT` geometry converted to WKB; the file has GeoParquet metadata so tools like DuckDB and GeoPandas read it as a geometry column
        - `mainstem_uri`: the uri of the mainstem associated with the geometry; only set for sitemaps that request `add_associated_mainstems` in the sitemap index
        - `bbox`: the GeoParquet 1.1 bbox covering of the geometry
    - `summoned/rdf/<sitemap_id>.parquet` has the JSON-LD for converting to RDF:
        - `id`: the `@id` of the document; it has a bloom filter so lookups by id can skip row groups
        - `jsonld`: the harvested JSON-LD document along with any enrichments added by Nabu, such as the associated mainstem and S2 cells
    - The url a document was harvested from is not stored since the `@id` is its canonical identifier
    - If the harvest of a sitemap fails, or no documents in it could be harvested, nothing is uploaded and the previous parquet files are kept as is
    - If a site fails on an error code that is non fatal and nabu will retry the http request. After multiple retries if the site still fails, Nabu will record the error and continue.
    - After crawling, Nabu validates the data is JSON-LD and validates it using SHACL. Only the first N SHACL validation errors will be stored so logs aren't spammed if every site fails the same way. 
    - With `--shacl-local`, Nabu validates in process against the bundled Geoconnex shape using [goRDFlib](https://github.com/tggo/goRDFlib); remote JSON-LD contexts are cached for the lifetime of the process and the schema.org context is bundled. Alternatively, `--shacl-grpc-endpoint` sends documents to an external SHACL validation service over gRPC
    - `nabu shacl serve` runs an HTTP validation service with the same `/validate` and `/shape` routes as the Python validator
    - For sitemaps that request it with `add_associated_mainstems` in the sitemap index, Nabu looks up the mainstem associated with each document's geometry in the `--mainstem-metadata` flatgeobuf file and adds it to the JSON-LD as a `hyf:referencedPosition` with the mainstem as its `hyf:linearElement`, so it is kept in the N-Quads. The mainstem is also stored in the `mainstem_uri` column so it can be used without parsing the JSON-LD. Harvesting a sitemap that requests mainstems without `--mainstem-metadata` is an error
    - For sitemaps that request it with `add_s2` in the sitemap index, Nabu adds every level 11 [S2 cell](http://s2geometry.io/devguide/s2cell_hierarchy) that each document's geometry intersects to its JSON-LD, using the [KnowWhereGraph S2 vocabulary](./s2_vocab.ttl). Each cell is linked with the most specific spatial relation from the [CGS spatiotemporal ontology](https://cgs-earth.github.io/ontologies/spatiotemporal/index.html) that holds for it: `cgs-st:sfWithin` when the geometry only intersects that cell (e.g. a point), `cgs-st:sfContains` when a polygon of the geometry contains the cell, `cgs-st:sfOverlaps` for the cells along the edge of a polygonal geometry, and otherwise the generic `cgs-st:spatialRelation` (e.g. a line crossing a cell, since the ontology has no `sfCrosses`). Each cell is described with its `kwg-ont:S2Cell_Level<N>` type and `kwg-ont:cellID`, and uses the same IRI as in KnowWhereGraph, i.e. `http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level11.<cell id>`. Whenever all four children of a cell intersect the geometry they are condensed into their parent, so a polygon may be associated with a mix of level 11 cells along its boundary and coarser cells inside it; every level 11 cell within a coarser cell intersects the geometry. Geometries that are not in longitude/latitude are kept without cells
    - Every harvest rewrites the whole parquet files, which only contain the documents fetched in that harvest. Documents for urls that are no longer in the sitemap, or that failed with a non fatal error, are not included
    - At the end of a crawl, Nabu puts a crawl report JSON file into the object store. This is used as the data source for the [crawl status page](../crawl-status-page/) so we don't need to add additional cloud infrastructure (i.e. a SQL db)

2. Nabu converts the JSON-LD in the RDF parquet files to N-Quads with `nabu nquads`
    - N-Quads are written to stdout so they can be streamed directly into a graph database via stdin, i.e. `nabu nquads | <loader>`
    - With no arguments, every parquet file under `--prefix` (default `summoned/rdf/`) in the bucket is converted; local RDF parquet files can be passed as arguments instead
    - Each document is put in its own named graph, `urn:iow:summoned:<sitemap_id>:<base64 of @id>.jsonld`
    - Nabu deterministically skolemizes blank nodes in RDF so each triple has a unique stable identifier for all terms

## Previous Architecture (no longer used)

Nabu previously synced with the graph database by crawling all JSON-LD and then doing a diff between all the items in the object store bucket and the live database. This architecture did not scale and has since been deprecated since:
    - diff'ing the state of the object store and the graph is extremely expensive to calculate
    - it required many round trips against with limited use of batch uploads
    - it required running computationally expensive queries against the production database

It is best to do all data cleaning operations using the object store as the source of truth / staging data store and then once the final data is prepared, upload it to the database in one large bulk upload. 