// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/internetofwater/nabu/internal/common/projectpath"

	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// NewJsonldProcessor builds the JSON-LD processor and sets the options object
// for use in framing, processing and all JSON-LD actions
func NewJsonldProcessor(cache bool, contextMaps map[string]string) (*ld.JsonLdProcessor, *ld.JsonLdOptions, error) {
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

		prefixToFullFilePath := make(map[string]string)

		for prefix, file := range contextMaps {
			// All context maps should be relative to the root of the project
			absPath := filepath.Join(projectpath.Root, file)
			if fileExists(absPath) {
				prefixToFullFilePath[prefix] = absPath
			} else {
				return nil, nil, fmt.Errorf("context file at %s does not exist or could not be accessed", absPath)
			}
		}

		// Read mapping from config file
		cachingLoader := ld.NewCachingDocumentLoader(fallbackLoader)
		if err := cachingLoader.PreloadWithMapping(prefixToFullFilePath); err != nil {
			return nil, nil, err
		}
		options.DocumentLoader = cachingLoader
	}

	options.ProcessingMode = ld.JsonLd_1_1 // add mode explicitly if you need JSON-LD 1.1 features
	options.Format = "application/nquads"  // Set to a default format. (make an option?)

	return processor, options, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Printf("error checking file existence: %v", err)
		return false
	}
	return !info.IsDir()
}

func JsonldToNQ(jsonld string, processor *ld.JsonLdProcessor, options *ld.JsonLdOptions) (string, error) {
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
func AddKeyToJsonLDContext(jsonld map[string]any, key, value string) (map[string]any, error) {
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
