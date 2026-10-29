// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/pkg"
	"github.com/stretchr/testify/require"
)

// records the uploads made to the storage it wraps
type recordingStorage struct {
	storage.CrawlStorage
	uploads int
}

func (r *recordingStorage) StoreWithHash(path storage.ObjectPath, data io.Reader, byteLength int) error {
	r.uploads++
	return r.CrawlStorage.StoreWithHash(path, data, byteLength)
}

func (r *recordingStorage) StoreWithoutServersideHash(path storage.ObjectPath, data io.Reader) error {
	r.uploads++
	return r.CrawlStorage.StoreWithoutServersideHash(path, data)
}

func newRecordingStorage(t *testing.T) *recordingStorage {
	local, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	return &recordingStorage{CrawlStorage: local}
}

// the names of the temporary files of a sink
func tempFileNames(sink *parquetSink) []string {
	names := []string{}
	for _, file := range sink.files {
		names = append(names, file.tmpFile.Name())
	}
	return names
}

func TestParquetSinkOnlyUploadsOnCommit(t *testing.T) {
	store := newRecordingStorage(t)
	sink, err := newSitemapParquetSink(store, "test")
	require.NoError(t, err)
	tmpNames := tempFileNames(sink)
	require.Len(t, tmpNames, 2)

	for _, id := range []string{"https://example.com/1", "https://example.com/2"} {
		require.NoError(t, sink.Add(parquettable.Feature{ID: id, JSONLD: []byte(`{"@id":"` + id + `"}`)}))
	}
	// nothing is uploaded while features are still being added, so a slow
	// document cannot leave an open upload idle
	require.Zero(t, store.uploads)

	rows, err := sink.Commit(pkg.SitemapCrawlStats{SitemapName: "test"})
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Equal(t, 2, store.uploads, "both the structured and RDF files should be uploaded")

	features, exists, err := readStoredFeatures(context.Background(), store, "test")
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, features, 2)
	require.Equal(t, "https://example.com/1", features[0].ID)
	require.Equal(t, `{"@id":"https://example.com/1"}`, string(features[0].JSONLD))

	for _, tmpName := range tmpNames {
		_, err = os.Stat(tmpName)
		require.True(t, errors.Is(err, os.ErrNotExist), "the temporary files should be removed after the upload")
	}
}

func TestParquetSinkAbortDoesNotUpload(t *testing.T) {
	store := newRecordingStorage(t)
	sink, err := newSitemapParquetSink(store, "test")
	require.NoError(t, err)
	tmpNames := tempFileNames(sink)

	require.NoError(t, sink.Add(parquettable.Feature{ID: "https://example.com/1", JSONLD: []byte(`{}`)}))
	sink.Abort(errors.New("harvest failed"))

	require.Zero(t, store.uploads)
	require.Error(t, sink.Add(parquettable.Feature{ID: "https://example.com/2", JSONLD: []byte(`{}`)}))
	for _, tmpName := range tmpNames {
		_, err = os.Stat(tmpName)
		require.True(t, errors.Is(err, os.ErrNotExist), "the temporary files should be removed after an abort")
	}
}
