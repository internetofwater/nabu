// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"errors"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGleanerTempFSCrawlStorage(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	// Store data
	err = storage.StoreWithoutServersideHash("testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)

	// Get data
	reader, err := storage.Get("testfile.txt")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "dummy_data", string(readData))

	// Check existence
	exists, err := storage.Exists("testfile.txt")
	require.NoError(t, err)
	require.True(t, exists)
}

// a reader that returns some data and then fails
type failingReader struct{ sent bool }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, errors.New("upstream failure")
	}
	r.sent = true
	return copy(p, "partial"), nil
}

func TestFailedStoreKeepsPreviousFile(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	require.NoError(t, storage.StoreWithoutServersideHash("dir/file.parquet", bytes.NewReader([]byte("original"))))
	require.Error(t, storage.StoreWithoutServersideHash("dir/file.parquet", &failingReader{}))

	reader, err := storage.Get("dir/file.parquet")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))

	set, err := storage.ListDir("dir")
	require.NoError(t, err)
	require.Len(t, set, 1, "no temporary files should be left behind")
}

func TestSet(t *testing.T) {
	set := make(Set)
	set.Add("testfile.txt")
	require.True(t, set.Contains("testfile.txt"))
	require.False(t, set.Contains("testfile2.txt"))

	require.Len(t, set, 1)
	set.Add("testfile3.txt")
	require.Len(t, set, 2)
}

func TestListDir(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	set, err := storage.ListDir("does-not-exist/")
	require.NoError(t, err)
	require.Empty(t, set)

	err = storage.StoreWithoutServersideHash("testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	set, err = storage.ListDir("")
	require.NoError(t, err)
	for item := range set {
		isAbs := path.IsAbs(item)
		require.True(t, isAbs, "ListDir paths should be absolute")
		require.Contains(t, item, "/testfile.txt")
	}
}
