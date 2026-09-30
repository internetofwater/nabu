// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tggo/goRDFlib/jsonld"
	"github.com/tggo/goRDFlib/shacl"
)

// a loader that counts fetches and returns a fixed context
type countingLoader struct {
	calls atomic.Int64
	fail  atomic.Bool
	delay time.Duration
}

func (c *countingLoader) LoadDocument(url string) (*ld.RemoteDocument, error) {
	c.calls.Add(1)
	time.Sleep(c.delay)
	if c.fail.Load() {
		return nil, errors.New("network down")
	}
	return &ld.RemoteDocument{DocumentURL: url, Document: map[string]any{
		"@context": map[string]any{"@vocab": "https://schema.org/"},
	}}, nil
}

func TestCachingDocumentLoaderFetchesOnce(t *testing.T) {
	next := &countingLoader{}
	loader := NewCachingDocumentLoader(next)

	for range 5 {
		doc, err := loader.LoadDocument("https://example.com/context")
		require.NoError(t, err)
		require.Equal(t, "https://example.com/context", doc.DocumentURL)
	}
	require.Equal(t, int64(1), next.calls.Load())
}

func TestCachingDocumentLoaderConcurrentFetchesAreShared(t *testing.T) {
	next := &countingLoader{delay: 50 * time.Millisecond}
	loader := NewCachingDocumentLoader(next)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, err := loader.LoadDocument("https://example.com/context")
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	require.Equal(t, int64(1), next.calls.Load())
}

func TestCachingDocumentLoaderDoesNotCacheFailures(t *testing.T) {
	next := &countingLoader{}
	next.fail.Store(true)
	loader := NewCachingDocumentLoader(next)

	_, err := loader.LoadDocument("https://example.com/context")
	require.Error(t, err)

	next.fail.Store(false)
	_, err = loader.LoadDocument("https://example.com/context")
	require.NoError(t, err)
	require.Equal(t, int64(2), next.calls.Load())
}

func TestDefaultCachingDocumentLoaderPreloadsSchemaOrg(t *testing.T) {
	loader, err := NewDefaultCachingDocumentLoader(nil)
	require.NoError(t, err)
	// replace the network loader so any fetch fails the test
	next := &countingLoader{}
	next.fail.Store(true)
	loader.next = next

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
	validator.SetDocumentLoader(NewCachingDocumentLoader(next))

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
