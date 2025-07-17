// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	log "github.com/sirupsen/logrus"
)

// returns the elements in `a` that aren't in `b`.
func findMissing(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func getTextBeforeDot(s string) string {
	n := strings.LastIndexByte(s, '.')
	if n == -1 {
		return s
	}
	return s[:n]
}

// given a prefix, return the name of the release graph
// that represents it
func makeReleaseNqName(prefix s3.S3Prefix) (string, error) {
	prefix_parts := strings.Split(prefix, "/")
	if len(prefix_parts) <= 1 {
		return "", fmt.Errorf("prefix %s did not contain a slash and thus is ambiguous", prefix)
	}
	// i.e. summoned/counties0 would become counties0
	prefix_path_as_filename := getTextBeforeDot(path.Base(strings.Join(prefix_parts[1:], "_")))

	var release_nq_name string
	if slices.Contains(prefix_parts, "summoned") && prefix_path_as_filename != "" {
		release_nq_name = fmt.Sprintf("%s_release.nq", prefix_path_as_filename) // ex: counties0_release.nq
	} else if slices.Contains(prefix_parts, "prov") && prefix_path_as_filename != "" {
		release_nq_name = fmt.Sprintf("%s_prov.nq", prefix_path_as_filename) // ex: counties0_prov.nq
	} else if slices.Contains(prefix_parts, "orgs") {
		if prefix_path_as_filename == "" {
			release_nq_name = "organizations.nq"
		} else {
			release_nq_name = fmt.Sprintf("%s_organizations.nq", prefix_path_as_filename)
		}
	} else {
		return "", fmt.Errorf("unable to form a release graph name from prefix %s", prefix)
	}
	return release_nq_name, nil
}

// Given a list of graph names, split them into an array of arrays of size batchSize
func createBatches(graphNames []s3.S3Prefix, batchSize int) [][]string {
	if batchSize == 0 {
		log.Warn("Got batch size of 0 so returning empty array")
		return [][]string{}
	}

	batches := [][]string{}
	for i := 0; i < len(graphNames); i += batchSize {
		end := min(i+batchSize, len(graphNames))
		batches = append(batches, graphNames[i:end])
	}
	return batches
}

// gzip is non deterministic; this is since it will add a
// header with a timestamp; we want to explicitly control this
// and make sure it is deterministic so our hashes are the same
func deterministicGzipWriter(w io.Writer) (*gzip.Writer, error) {
	gzipWriter, err := gzip.NewWriterLevel(w, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	gzipWriter.Header.ModTime = time.Unix(0, 0) // deterministic timestamp
	gzipWriter.Header.Comment = ""
	gzipWriter.Header.Extra = nil
	gzipWriter.Header.Name = ""
	gzipWriter.Header.OS = 255 // optional: avoid platform-specific bytes
	return gzipWriter, nil
}

// Consume the nqChan and write to the pipeWriter; return the hash of all the data that
// was written to that pipe
func writeToPipeAndGetHash(compress bool, nqChan <-chan string, pipeWriter *io.PipeWriter) (string, error) {
	hashDestination := sha256.New()
	var writer io.Writer
	var zipper *gzip.Writer

	if compress {
		// Create multiwriter to write compressed bytes to pipeWriter and also hash them
		mw := io.MultiWriter(pipeWriter, hashDestination)

		gzipWriter, err := deterministicGzipWriter(mw)
		if err != nil {
			return "", err
		}
		zipper = gzipWriter
		writer = gzipWriter
	} else {
		writer = io.MultiWriter(pipeWriter, hashDestination)
	}

	for nq := range nqChan {
		asBytes := []byte(nq)
		_, err := writer.Write(asBytes)
		if err != nil {
			pipeWriter.CloseWithError(err)
			return "", err
		}
	}
	if zipper != nil {
		if err := zipper.Close(); err != nil {
			return "", err
		}
	}
	if err := pipeWriter.Close(); err != nil {
		return "", err
	}

	hash := hex.EncodeToString(hashDestination.Sum(nil))
	return hash, nil
}
