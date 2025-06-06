// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"io"
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
}
