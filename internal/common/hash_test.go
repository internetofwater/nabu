// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
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

func TestSumWriter_Write(t *testing.T) {
	sw := SumWriter{}

	data := []byte{1, 2, 3, 4, 5} // 1+2+3+4+5 = 15
	n, err := sw.Write(data)

	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, uint64(15), sw.Sum)
	require.Equal(t, "15", sw.ToString())
}

func TestSumWriter_EmptyWrite(t *testing.T) {
	sw := SumWriter{}

	data := []byte{} // Empty slice
	n, err := sw.Write(data)

	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, uint64(0), sw.Sum)
	require.Equal(t, "0", sw.ToString())
}

func TestSumWriterWrapAround(t *testing.T) {
	sw := &SumWriter{}
	sw.Sum = math.MaxUint64

	data := []byte{1} // This pushes the sum past the max value, causing wraparound
	n, err := sw.Write(data)

	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, uint64(0), sw.Sum) // 4294967295 + 1 = 0 (wraps)
	require.Equal(t, "0", sw.ToString())
}
