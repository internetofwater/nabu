// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrphanAndMissing(t *testing.T) {

	s3MockNames := []string{"a", "b", "c", "d", "e"}
	tripleStoreMockNames := []string{"a", "b", "c", "g", "h"}

	missingToAdd := findMissing(s3MockNames, tripleStoreMockNames)
	require.Equal(t, []string{"d", "e"}, missingToAdd)

	orphaned := findMissing(tripleStoreMockNames, s3MockNames)
	require.Equal(t, []string{"g", "h"}, orphaned)
}

func TestGetTextBeforeDot(t *testing.T) {
	res := getTextBeforeDot("test.go")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test.go.go")
	require.Equal(t, "test.go", res)
}

func TestMakeReleaseName(t *testing.T) {

	res, err := makeReleaseNqName("summoned/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_release.nq", res)

	res, err = makeReleaseNqName("prov/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_prov.nq", res)

	res, err = makeReleaseNqName("orgs/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_organizations.nq", res)
	res, err = makeReleaseNqName("orgs/")
	require.NoError(t, err)
	require.Equal(t, "organizations.nq", res)

	_, err = makeReleaseNqName("orgs")
	require.Error(t, err)
}

func TestAllocateBatches(t *testing.T) {
	tests := []struct {
		name       string
		graphNames []string
		batchSize  int
		want       [][]string
	}{
		{
			name:       "even split",
			graphNames: []string{"a", "b", "c", "d"},
			batchSize:  2,
			want:       [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:       "uneven split",
			graphNames: []string{"a", "b", "c", "d", "e"},
			batchSize:  2,
			want:       [][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
		{
			name:       "batch size greater than input",
			graphNames: []string{"a", "b"},
			batchSize:  5,
			want:       [][]string{{"a", "b"}},
		},
		{
			name:       "batch size is 1",
			graphNames: []string{"a", "b", "c"},
			batchSize:  1,
			want:       [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			name:       "empty input",
			graphNames: []string{},
			batchSize:  3,
			want:       [][]string{}, // Ensure this matches the return value: [][]string{}
		},
		{
			name:       "batch size is 0",
			graphNames: []string{"a", "b"},
			batchSize:  0,
			want:       [][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createBatches(tt.graphNames, tt.batchSize)
			require.Equal(t, tt.want, got)
		})
	}
}

func compressWithDeterministicWriter(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := deterministicGzipWriter(&buf)
	if err != nil {
		return nil, err
	}
	_, err = writer.Write(data)
	if err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Make sure that the gzip writer is deterministic and that the hash is the same
// for the same input
func TestDeterministicGzipWriter(t *testing.T) {
	input := []byte("The quick brown fox jumps over the lazy dog.")

	output1, err := compressWithDeterministicWriter(input)
	require.NoError(t, err, "first compression failed")

	require.NotEqual(t, input, output1, "gzip output should be different from input")

	output2, err := compressWithDeterministicWriter(input)
	require.NoError(t, err, "second compression failed")

	require.Equal(t, output1, output2, "gzip output should be deterministic")

	// Compare SHA-256 hashes
	hash1 := sha256.Sum256(output1)
	hash2 := sha256.Sum256(output2)

	require.Equal(t, hash1, hash2, "SHA-256 hashes of gzip output should match")
}

func runHashTest(t *testing.T, compress bool, inputData []string) string {
	nqChan := make(chan string)
	pipeReader, pipeWriter := io.Pipe()

	// Drain the pipe in a goroutine to prevent blocking
	go func() {
		_, err := io.Copy(io.Discard, pipeReader)
		require.NoError(t, err)
	}()

	go func() {
		defer close(nqChan)
		for _, s := range inputData {
			nqChan <- s
		}
	}()

	hash, err := writeToPipeAndGetHash(compress, nqChan, pipeWriter)
	require.NoError(t, err)
	require.NotEmpty(t, hash)
	return hash
}

func TestWriteToPipeAndGetHash_Deterministic(t *testing.T) {
	input := []string{"hello", "world", "foo", "bar"}

	// Run twice with compression enabled
	hash1 := runHashTest(t, true, input)
	hash2 := runHashTest(t, true, input)
	require.Equal(t, hash1, "415ba8f2b3fd2f349a934136625c2f65ad89958540291a9c36fab8975ee3b98b")
	require.Equal(t, hash1, hash2, "Hashes should match with compression")

	// Run twice without compression
	hash3 := runHashTest(t, false, input)
	hash4 := runHashTest(t, false, input)
	require.Equal(t, hash3, "7f80c7249ffbf860aa08202d3a9d62625c726d9a9706471fa54755d283d969ee")
	require.Equal(t, hash3, hash4, "Hashes should match without compression")

	// Sanity check: different compression settings should produce different hashes
	require.NotEqual(t, hash1, hash3, "Compressed and uncompressed hashes should differ")
}

func TestSumWriter_Write(t *testing.T) {
	sw := SumWriter{}

	data := []byte{1, 2, 3, 4, 5} // 1+2+3+4+5 = 15
	n, err := sw.Write(data)

	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, uint32(15), sw.Sum)
	assert.Equal(t, "15", sw.ToString())
}

func TestSumWriter_EmptyWrite(t *testing.T) {
	sw := SumWriter{}

	data := []byte{} // Empty slice
	n, err := sw.Write(data)

	assert.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, uint32(0), sw.Sum)
	assert.Equal(t, "0", sw.ToString())
}

func TestSumWriterWrapAround(t *testing.T) {
	sw := &SumWriter{}
	sw.Sum = math.MaxUint32

	data := []byte{1} // This pushes the sum past the max value, causing wraparound
	n, err := sw.Write(data)

	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, uint32(0), sw.Sum) // 4294967295 + 1 = 0 (wraps)
	assert.Equal(t, "0", sw.ToString())
}
