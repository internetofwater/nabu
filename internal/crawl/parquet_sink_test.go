// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
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

func TestParquetSinkOnlyUploadsOnCommit(t *testing.T) {
	store := newRecordingStorage(t)
	sink, err := newParquetSink(store, "summoned/test.parquet")
	require.NoError(t, err)
	tmpName := sink.tmpFile.Name()

	for _, id := range []string{"https://example.com/1", "https://example.com/2"} {
		require.NoError(t, sink.Add(parquettable.Feature{ID: id, JSONLD: []byte(`{"@id":"` + id + `"}`)}))
	}
	// nothing is uploaded while features are still being added, so a slow
	// document cannot leave an open upload idle
	require.Zero(t, store.uploads)

	rows, err := sink.Commit(pkg.SitemapCrawlStats{SitemapName: "test"})
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Equal(t, 1, store.uploads)

	reader, err := store.Get("summoned/test.parquet")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "PAR1", string(data[len(data)-4:]))

	_, err = os.Stat(tmpName)
	require.True(t, errors.Is(err, os.ErrNotExist), "the temporary file should be removed after the upload")
}

func TestParquetSinkAbortDoesNotUpload(t *testing.T) {
	store := newRecordingStorage(t)
	sink, err := newParquetSink(store, "summoned/test.parquet")
	require.NoError(t, err)
	tmpName := sink.tmpFile.Name()

	require.NoError(t, sink.Add(parquettable.Feature{ID: "https://example.com/1", JSONLD: []byte(`{}`)}))
	sink.Abort(errors.New("harvest failed"))

	require.Zero(t, store.uploads)
	require.Error(t, sink.Add(parquettable.Feature{ID: "https://example.com/2", JSONLD: []byte(`{}`)}))
	_, err = os.Stat(tmpName)
	require.True(t, errors.Is(err, os.ErrNotExist), "the temporary file should be removed after an abort")
}
