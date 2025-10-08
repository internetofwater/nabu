// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGleanerTempFSCrawlStorage(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	// Store data
	err = storage.Store("testfile.txt", bytes.NewReader([]byte("dummy_data")))
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

	isEmpty, err := storage.IsEmptyDir("dummy_nonexistent_directory/")
	require.NoError(t, err)
	require.True(t, isEmpty)

	isEmpty, err = storage.IsEmptyDir("")
	require.NoError(t, err)
	require.False(t, isEmpty)
}

func TestSet(t *testing.T) {
	set := make(Set)
	set.Add("testfile.txt")
	require.True(t, set.Contains("testfile.txt"))
	require.False(t, set.Contains("testfile2.txt"))
}

func TestListDir(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	err = storage.Store("testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	set, err := storage.ListDir("")
	require.NoError(t, err)
	for item := range set {
		isAbs := path.IsAbs(item)
		require.True(t, isAbs, "ListDir paths should be absolute")
		require.Contains(t, item, "/testfile.txt")
	}
}

func TestCleanupOldJsonld(t *testing.T) {
	storage, err := NewLocalTempFSCrawlStorage()
	// setup
	require.NoError(t, err)
	err = storage.Store("summoned/sitemap1/testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	filesinStorage := make(Set)

	// "make sure files that are seen are kept"
	filesinStorage.Add("summoned/sitemap1/testfile.txt")
	_, err = CleanupFiles("summoned/sitemap1", filesinStorage, storage)
	require.NoError(t, err)
	res, err := storage.Exists("summoned/sitemap1/testfile.txt")
	require.NoError(t, err)
	require.True(t, res, "File should still exist since it was in the set")

	// "make sure files that are not seen are removed"
	err = storage.Store("summoned/sitemap1/THIS_SHOULD_BE_REMOVED.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	_, err = CleanupFiles("summoned/sitemap1", filesinStorage, storage)
	require.NoError(t, err)
	res, err = storage.Exists("summoned/sitemap1/THIS_SHOULD_BE_REMOVED.txt")
	require.NoError(t, err)
	require.False(t, res)

	// make sure files that in a different path are not touched", func(t *testing.T)
	err = storage.Store("summoned/sitemap2/KEEP_THIS.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	_, err = CleanupFiles("summoned/sitemap1", filesinStorage, storage)
	require.NoError(t, err)
	res, err = storage.Exists("summoned/sitemap2/KEEP_THIS.txt")
	require.NoError(t, err)
	require.True(t, res)
}
