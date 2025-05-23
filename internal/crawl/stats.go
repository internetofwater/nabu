// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"encoding/json"
	"fmt"
)

// An error for a particular URL in a sitemap
type UrlCrawlError struct {
	Url     string
	Status  int
	Message string
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
