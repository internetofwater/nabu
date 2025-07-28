// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

	client := NewCrawlerClient()

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

	client := NewCrawlerClient()

	req, err := http.NewRequest("GET", server.URL, nil)
	require.NoError(t, err)

	resp, err := client.Do(req)

	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, int32(1), callCount, "404 should not be retried")
}

func TestMockWithString(t *testing.T) {

	mock := NewMockedClient(map[string]MockResponse{
		"http://example.com": {
			statusCode: 200,
			body:       "success",
		},
	})

	resp, err := mock.Get("http://example.com")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 200, resp.StatusCode)
	readBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "success", string(readBody))
}

func TestMockWithFile(t *testing.T) {

	mock := NewMockedClient(map[string]MockResponse{
		"http://example.com": {
			statusCode: 404,
			file:       "testdata/mock_file",
		},
	})

	resp, err := mock.Get("http://example.com")
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 404, resp.StatusCode)
	readBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, string(readBody), "This is a mock file")
}
