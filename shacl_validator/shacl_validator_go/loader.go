// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/internetofwater/nabu/shacl_validator/contexts"
	"github.com/piprate/json-gold/ld"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// CachingDocumentLoader caches remote JSON-LD documents, such as a remote
// @context, for the lifetime of the process so each url is fetched at most once
// instead of on every parse. Unlike ld.CachingDocumentLoader it is safe for concurrent use.
// Cached documents are shared between parses; json-gold only reads them
type CachingDocumentLoader struct {
	next     ld.DocumentLoader
	mu       sync.RWMutex
	cache    map[string]*ld.RemoteDocument
	inflight singleflight.Group
}

// NewCachingDocumentLoader returns a loader that caches documents fetched by next
func NewCachingDocumentLoader(next ld.DocumentLoader) *CachingDocumentLoader {
	return &CachingDocumentLoader{
		next:  next,
		cache: make(map[string]*ld.RemoteDocument),
	}
}

// NewDefaultCachingDocumentLoader returns a caching loader preloaded with the
// bundled schema.org context that fetches any other documents with client.
// If client is nil a client with a timeout is used
func NewDefaultCachingDocumentLoader(client *http.Client) (*CachingDocumentLoader, error) {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	loader := NewCachingDocumentLoader(ld.NewDefaultDocumentLoader(client))
	if err := loader.Preload(contexts.SchemaOrgContext, contexts.SchemaOrgContextUrls...); err != nil {
		return nil, err
	}
	return loader, nil
}

// Preload caches a JSON-LD document under each of the given urls
// so that it is never fetched over the network
func (l *CachingDocumentLoader) Preload(document []byte, urls ...string) error {
	doc, err := ld.DocumentFromReader(bytes.NewReader(document))
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, url := range urls {
		l.cache[url] = &ld.RemoteDocument{DocumentURL: url, Document: doc}
	}
	return nil
}

// LoadDocument returns the cached document for the url or fetches and caches it.
// Concurrent requests for the same uncached url share one fetch; failed fetches are not cached
func (l *CachingDocumentLoader) LoadDocument(url string) (*ld.RemoteDocument, error) {
	l.mu.RLock()
	doc, ok := l.cache[url]
	l.mu.RUnlock()
	if ok {
		return doc, nil
	}

	result, err, _ := l.inflight.Do(url, func() (any, error) {
		doc, err := l.next.LoadDocument(url)
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		l.cache[url] = doc
		l.mu.Unlock()
		log.Debugf("Cached remote JSON-LD document %s", url)
		return doc, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*ld.RemoteDocument), nil
}
