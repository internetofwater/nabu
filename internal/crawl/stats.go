// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"encoding/json"
	"fmt"
)

// An error for a particular URL in a sitemap
type UrlCrawlError struct {
	// The URL that failed
	Url string
	// The http status code of the fetched Url
	Status int
	// a natural language error message describing the error
	Message string
	// whether the shacl validation succeeded
	ShaclValid bool
	// the shacl validation message
	ShaclErrorMessage string
}

func (e UrlCrawlError) Error() string {
	return fmt.Errorf("failed to crawl %s with status %d: %s", e.Url, e.Status, e.Message).Error()
}

// Crawl stats for a particular sitemap
// Which can optionally include a list of errors
type SitemapCrawlStats struct {
	CrawlFailures     []UrlCrawlError
	SecondsToComplete float64
	SitemapName       string
}

func ToJson(stats []SitemapCrawlStats) string {
	data, err := json.Marshal(stats)
	if err != nil {
		// Fallback to empty array JSON on error
		return "[]"
	}
	return string(data)
}
