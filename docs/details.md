# Technical Details

## Performance

Performance of Nabu is highly dependent upon your S3 and graph database. Parallelism can be configured via CLI flags if Nabu is taking up too many resources.

## Properties

### Harvesting

While harvesting, Nabu:

1. Reads the sitemap
2. Fetches the JSON-LD from each page
3. Generates a hash of the JSON-LD
4. Checks if the hash already exists in the triplestore
    - If it does, the page is skipped
5. Validates the JSON-LD is conformant to the desired shacl shapes
    - If not, the page is skipped and an error is logged
6. Stores it in the S3 bucket

### Synchronization 

While synchronizing data, Nabu:

- does not try to manage state of inidividual triples inside the triplestore. If there is any potential for conflict, Nabu just drops the entire graph and all its associated triples
- uses the s3 bucket is used as the source of truth
    - If an item from one crawl is present in the S3 but it disappears in the next crawl, it will not continue to be present in the triplestore after running nabu synchronization
        - This is since Nabu never removes or mutates items in S3
- uses the context item (the fourth context term of the triple) to associate each triple with a graph and thus an individual source
    - It simply checks if a named graph exists and will drop it before inserting triples of the same graph
- does not use any caching when syncing and there is no concept of statefulness from one nabu run to the other
- is agnostic to the content contained inside individual jsonld/nq files as long as they are syntactically valid and passed shacl validation