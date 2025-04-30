// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewRetryableHTTPClient verifies that the retryable HTTP client retries failed requests.
func TestNewRetryableHTTPClient(t *testing.T) {
	var requestCount int32

	// Create a test server that fails the first two requests and succeeds on the third
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError) // Simulate transient failure (HTTP 500)
			return
		}
		w.WriteHeader(http.StatusOK) // Success on the third attempt
	}))
	defer server.Close()

	client := NewRetryableHTTPClient()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	// Ensure request was eventually successful
	if err != nil {
		t.Fatalf("Request failed after retries: %v", err)
	}

	// Check that the response was successful (HTTP 200)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected HTTP 200, got %d", resp.StatusCode)
	}

	// Check that it retried the correct number of times (should be 3)
	if requestCount != 3 {
		t.Errorf("Expected 3 attempts, but got %d", requestCount)
	}

	// Ensure that retries did not take excessively long
	if elapsed > 7*time.Second {
		t.Errorf("Retries took too long: %v", elapsed)
	}
}
