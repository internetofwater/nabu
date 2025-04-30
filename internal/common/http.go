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
	retryClient.RetryWaitMax = 5 * time.Second

	return retryClient.StandardClient() // Convert to *http.Client so we can use it in the jsonld loader
}
