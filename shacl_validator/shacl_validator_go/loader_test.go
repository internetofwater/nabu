// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"
	"github.com/tggo/goRDFlib/jsonld"
	"github.com/tggo/goRDFlib/shacl"
)

// a loader that counts fetches and returns a fixed context
type countingLoader struct {
	calls atomic.Int64
	fail  atomic.Bool
}

func (c *countingLoader) LoadDocument(url string) (*ld.RemoteDocument, error) {
	c.calls.Add(1)
	if c.fail.Load() {
		return nil, errors.New("network down")
	}
	return &ld.RemoteDocument{DocumentURL: url, Document: map[string]any{
		"@context": map[string]any{"@vocab": "https://schema.org/"},
	}}, nil
}

func TestDefaultCachingDocumentLoaderPreloadsSchemaOrg(t *testing.T) {
	// a fallback loader that fails so any network fetch fails the test
	next := &countingLoader{}
	next.fail.Store(true)
	loader, err := newSchemaOrgCachingDocumentLoader(next)
	require.NoError(t, err)

	const doc = `{"@context": "https://schema.org/", "@id": "https://example.com/1", "@type": "Place", "name": "x"}`
	graph, err := shacl.LoadJsonLDString(doc, "", jsonld.WithDocumentLoader(loader))
	require.NoError(t, err)
	require.Zero(t, next.calls.Load(), "the schema.org context should never be fetched over the network")

	// the schema.org context uses an http vocab
	names := graph.Objects(shacl.IRI("https://example.com/1"), shacl.IRI("http://schema.org/name"))
	require.Len(t, names, 1)
}

func TestValidatorUsesDocumentLoader(t *testing.T) {
	validator, err := NewGeoconnexShaclValidator()
	require.NoError(t, err)
	next := &countingLoader{}
	validator.SetDocumentLoader(jsonld.NewCachingDocumentLoader(next))

	const doc = `{"@context": "https://example.com/context", "@id": "https://example.com/1", "@type": "Place", "name": "x"}`
	for range 3 {
		report, err := validator.ValidateJsonldString(doc)
		require.NoError(t, err)
		// the remote context resolved, so the node is typed as a place and validated against the shape
		require.False(t, report.Conforms)
		require.NotContains(t, ReportText(report), MissingPlaceOrDatasetTypeMessage)
	}
	require.Equal(t, int64(1), next.calls.Load())
}
