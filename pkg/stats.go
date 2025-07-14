// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// The status of the SHACL validation
type ShaclStatus string

const (
	ShaclSkipped ShaclStatus = "skipped"
	ShaclInvalid ShaclStatus = "invalid"
	ShaclValid   ShaclStatus = "valid"
)

// An error for a particular URL in a sitemap
type UrlCrawlError struct {
	Url               string      `json:"url"`          // schema.org/url
	Status            int         `json:"statusCode"`   // loosely schema.org/statusCode
	Message           string      `json:"description"`  // schema.org/description
	ShaclStatus       ShaclStatus `json:"shaclStatus"`  // unmapped
	ShaclErrorMessage string      `json:"shaclMessage"` // unmapped
}

func (e UrlCrawlError) Error() string {
	return fmt.Sprintf("failed to crawl %s; status %d, message: %s, shacl status: %s, shacl message: %s",
		e.Url, e.Status, e.Message, e.ShaclStatus, e.ShaclErrorMessage)
}

// Crawl stats for a particular sitemap
type SitemapCrawlStats struct {
	Type              string          `json:"@type"`                  // schema.org type
	SitemapName       string          `json:"name"`                   // schema.org/name
	SuccessfulUrls    []string        `json:"successfulUrls"`         // schema.org/url (plural)
	CrawlFailures     []UrlCrawlError `json:"crawlFailures"`          // unmapped
	SecondsToComplete float64         `json:"duration"`               // schema.org/duration (in seconds)
	SitesHarvested    int             `json:"numberOfSitesHarvested"` // unmapped
	SitesInSitemap    int             `json:"numberOfSitesInSitemap"` // unmapped
}

// Create a basic JSON-LD context map
func GetJsonLDContext() map[string]any {
	return map[string]any{
		"@vocab":         "https://schema.org/",
		"successfulUrls": "url",
	}
}

// Return a JSON-LD-compatible io.Reader for a single sitemap (still with context for backwards compat)
func (s SitemapCrawlStats) ToJsonIoReader() (io.Reader, error) {
	s.Type = "DataFeed"
	// Wrap single item in a document with top-level @context
	wrapped := map[string]any{
		"@context": GetJsonLDContext(),
		"@graph":   []SitemapCrawlStats{s},
	}
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(wrapped)
	return buf, err
}

// A sitemap index is just a list of sitemaps
type SitemapIndexCrawlStats []SitemapCrawlStats

// Serialize the sitemap index crawl stats to proper JSON-LD
func (s SitemapIndexCrawlStats) ToJson() (string, error) {
	for i := range s {
		s[i].Type = "DataFeed"
	}
	output := map[string]any{
		"@context": GetJsonLDContext(),
		"@graph":   s,
	}
	data, err := json.Marshal(output)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
