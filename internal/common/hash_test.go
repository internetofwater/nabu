// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteAndReturnSHA256(t *testing.T) {
	// Test data
	data := []byte("hello world")

	// Input reader and output writer
	input := bytes.NewReader(data)
	var output bytes.Buffer

	// Call the function
	hash, err := WriteAndReturnSHA256(&output, input)
	require.NoError(t, err, "expected no error from writeAndReturnSHA256")

	// Validate the written data
	require.Equal(t, data, output.Bytes(), "output data should match input")

	// Validate the SHA256 hash
	expectedHashBytes := sha256.Sum256(data)
	expectedHash := hex.EncodeToString(expectedHashBytes[:])
	require.Equal(t, expectedHash, hash, "SHA256 hash mismatch")
}
