package common

import (
	"encoding/json"
	"fmt"
	"nabu/internal/common/projectpath"
	"nabu/pkg/config"
	"os"
	"path/filepath"

	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
)

// NewJsonldProcessor builds the JSON-LD processor and sets the options object
// for use in framing, processing and all JSON-LD actions
func NewJsonldProcessor(config config.NabuConfig) (*ld.JsonLdProcessor, *ld.JsonLdOptions, error) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")

	if config.Context.Cache {
		// my understanding is that the fallbackLoader is what is used if
		// the prefix cannot be retrieved from the cache.
		clientWithRetries := NewRetryableHTTPClient()
		fallbackLoader := ld.NewDefaultDocumentLoader(clientWithRetries)

		prefixToFilePath := make(map[string]string)

		for _, contextMap := range config.ContextMaps {
			// All context maps should be relative to the root of the project
			absPath := filepath.Join(projectpath.Root, contextMap.File)
			if fileExists(absPath) {
				prefixToFilePath[contextMap.Prefix] = absPath
			} else {
				return nil, nil, fmt.Errorf("context file at %s does not exist or could not be accessed", absPath)
			}
		}

		// Read mapping from config file
		cachingLoader := ld.NewCachingDocumentLoader(fallbackLoader)
		if err := cachingLoader.PreloadWithMapping(prefixToFilePath); err != nil {
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
