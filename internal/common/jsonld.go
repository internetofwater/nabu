// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
	"github.com/tggo/goRDFlib/jsonld"
)

// NewJsonldProcessor builds the JSON-LD processor and sets the options object
// for use in framing, processing and all JSON-LD actions
func NewJsonldProcessor(cache bool) (*ld.JsonLdProcessor, *ld.JsonLdOptions, error) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")

	if cache {
		// my understanding is that the fallbackLoader is what is used if
		// the prefix cannot be retrieved from the cache.

		// TODO: check if we want a different client transport here
		// since the go default client limits maxconns to 100
		// assume it is fine though since the context is cached
		clientWithRetries := NewCrawlerClient()
		fallbackLoader := ld.NewDefaultDocumentLoader(clientWithRetries)

		// unlike ld.CachingDocumentLoader this loader
		// is safe to share across concurrent conversions
		options.DocumentLoader = jsonld.NewCachingDocumentLoader(fallbackLoader)
	}

	options.ProcessingMode = ld.JsonLd_1_1 // add mode explicitly if you need JSON-LD 1.1 features
	options.Format = "application/nquads"  // Set to a default format. (make an option?)

	return processor, options, nil
}

func JsonldToTriples(jsonld string, processor *ld.JsonLdProcessor, options *ld.JsonLdOptions) (string, error) {
	var deserializeInterface interface{}
	err := json.Unmarshal([]byte(jsonld), &deserializeInterface)
	if err != nil {
		log.Error("Error when transforming JSON-LD document to interface:", err)
		return "", err
	}
	triples, err := processor.ToRDF(deserializeInterface, options) // returns triples but toss them, just validating
	if err != nil {
		log.Error("Error when transforming JSON-LD document to RDF:", err)
		return "", err
	}

	return fmt.Sprintf("%v", triples), err
}

// Given a jsonld map, add a key to the context
func AddKeyToJsonLDContext(jsonld map[string]any, key, value string) (newJsonld map[string]any, err error) {
	context, ok := jsonld["@context"]
	if !ok {
		return nil, fmt.Errorf("JSON-LD document does not have @context field")
	}

	// since go doesn't have type narrowing or algebraic data types
	// we have to check the type of the context field manually with repeated
	// code which is ugly but works
	arrayMap, ok := context.([]any)
	if ok {
		arrayMap = append(arrayMap, map[string]string{key: value})
		jsonld["@context"] = arrayMap
		return jsonld, nil
	}
	contextMap, ok := context.(map[string]any)
	if ok {
		contextMap[key] = value
		jsonld["@context"] = contextMap
		return jsonld, nil
	}
	stringContextMap, ok := context.(map[string]string)
	if ok {
		stringContextMap[key] = value
		jsonld["@context"] = stringContextMap
		return jsonld, nil
	}

	stringContext, ok := context.(string)
	if ok {
		jsonld["@context"] = map[string]any{"@vocab": stringContext, key: value}

	}
	return nil, fmt.Errorf("JSON-LD had type %s for @context field and could not be modified", reflect.TypeOf(context))
}

// Get the wkt geometry from the
func GetWktFromJsonld(jsonld map[string]any) (wkt string, hasGeometry bool) {
	schema_geo, ok := jsonld["gsp:hasGeometry"].(map[string]any)
	if ok {
		geo, ok := schema_geo["gsp:asWKT"].(map[string]any)
		if ok {
			wkt, ok := geo["@value"].(string)
			if ok {
				return wkt, true
			}
		}
	}

	return "", false
}
