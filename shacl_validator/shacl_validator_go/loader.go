// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"bytes"
	"net/http"

	"github.com/internetofwater/nabu/shacl_validator/contexts"
	"github.com/piprate/json-gold/ld"
	"github.com/tggo/goRDFlib/jsonld"
)

// NewDefaultCachingDocumentLoader returns a concurrency safe loader that caches
// remote JSON-LD documents, such as a remote @context, for the lifetime of the process.
// The bundled schema.org context is preloaded so it is never fetched over the network.
// Any other document is fetched with client; if client is nil a client with a timeout is used
func NewDefaultCachingDocumentLoader(client *http.Client) (*jsonld.CachingDocumentLoader, error) {
	var next ld.DocumentLoader
	if client != nil {
		next = ld.NewDefaultDocumentLoader(client)
	}
	return newSchemaOrgCachingDocumentLoader(next)
}

// newSchemaOrgCachingDocumentLoader returns a caching loader preloaded with the
// schema.org context that falls back to next; if next is nil goRDFlib's default is used
func newSchemaOrgCachingDocumentLoader(next ld.DocumentLoader) (*jsonld.CachingDocumentLoader, error) {
	schemaOrgContext, err := ld.DocumentFromReader(bytes.NewReader(contexts.SchemaOrgContext))
	if err != nil {
		return nil, err
	}
	loader := jsonld.NewCachingDocumentLoader(next)
	loader.Preload(schemaOrgContext, contexts.SchemaOrgContextUrls...)
	return loader, nil
}
