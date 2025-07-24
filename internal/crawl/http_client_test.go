// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	url := "https://geoconnex.us/ref/hu04/0316"

	client := NewCrawlerHttpClient()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Add headers
	req.Header.Set("Want-Content-Digest", "sha256")
	req.Header.Set("Accept", "application/json+ld")

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	// Read all body bytes
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	// Compute actual SHA256 hash of response body
	hash := sha256.Sum256(bodyBytes)
	actualHash := hex.EncodeToString(hash[:])

	// Get expected hash from Content-Digest header (case-insensitive)
	contentDigest := resp.Header.Get("Content-Digest")
	if contentDigest == "" {
		t.Fatal("Content-Digest header not found")
	}

	var expectedHash string

	if strings.HasPrefix(strings.ToLower(contentDigest), "sha256=") {
		expectedHash = strings.TrimPrefix(contentDigest, "sha256=")
	}

	fmt.Println("Expected:", expectedHash)
	fmt.Println("Actual  :", actualHash)

	if expectedHash == actualHash {
		fmt.Println("✅ Hashes match.")
	} else {
		fmt.Println("❌ Hashes do not match!")
		t.Fail()
	}
}
func TestRetrySucceedsAfterFailures(t *testing.T) {
	var callCount int32 = 0

	// Fail 2 times, then succeed
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n <= 2 {
			http.Error(w, "temporary failure", http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("success"))
		}
	}))
	defer server.Close()

	client := &RetriableCrawlerHttpClient{
		client:  &http.Client{},
		retries: 3,
		backoff: 50 * time.Millisecond,
	}

	req, err := http.NewRequest("GET", server.URL, nil)
	require.NoError(t, err)

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	require.NoError(t, err, "expected successful response after retries")
	require.NotNil(t, resp, "response should not be nil")
	require.Equal(t, 200, int(resp.StatusCode), "should return 200 after retries")
	require.GreaterOrEqual(t, int(callCount), 3, "should retry at least twice")
	require.GreaterOrEqual(t, elapsed.Milliseconds(), int64(100), "should have waited for backoff")
}

func TestNoRetryOn404(t *testing.T) {
	var callCount int32 = 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := &RetriableCrawlerHttpClient{
		client:  &http.Client{},
		retries: 3,
		backoff: 50 * time.Millisecond,
	}

	req, err := http.NewRequest("GET", server.URL, nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, int32(1), callCount, "404 should not be retried")
}
