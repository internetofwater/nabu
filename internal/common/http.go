// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// NewRetryableHTTPClient returns an HTTP client with automatic retries.
func NewRetryableHTTPClient() *http.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.RetryWaitMin = 1 * time.Second
	retryClient.RetryWaitMax = 10 * time.Second
	// don't spam in the logs with DEBUG messages
	// we should define logs in the application
	// not the library level
	retryClient.Logger = nil

	return retryClient.StandardClient() // Convert to *http.Client so we can use it in the jsonld loader
}
