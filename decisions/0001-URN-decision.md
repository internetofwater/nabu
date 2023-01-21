# 1. Record architecture decisions

Date: 05-01-2023

## Status

Proposed

## Context

URNs for the graph URI are set in the file internal/graph/mintURN.go

## Decision

Old URNs were varationas on 

```rdf
urn:gleaner.oih:summoned:edmo:0255293683036aac2a95a2479cc841189c0ac3f8
```

or

```rdf
urn:gleaner.oih:milled:edmo:0255293683036aac2a95a2479cc841189c0ac3f8
```

The milled and summoned elements were pointless and led to confusion and were not 
really important in terms of getting to the object.  

The new desired URN pattern is 

```rdf
urn:gleaner.oh:edmo:0255293683036aac2a95a2479cc841189c0ac3f8
```

## Consequences

This impacts gleaner in the generation of prov which will need to use this same pattern
to fill out the prov records.  