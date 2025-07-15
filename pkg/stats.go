// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package pkg

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
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
	Url               string      `json:"@id"`                // schema.org/url
	Status            int         `json:"http:statusCodes"`   //
	Message           string      `json:"schema:description"` // schema.org/description
	ShaclStatus       ShaclStatus `json:"sh:resultSeverity"`  // unmapped
	ShaclErrorMessage string      `json:"sh:resultMessage"`   // unmapped
}

func (e UrlCrawlError) Error() string {
	return fmt.Sprintf("failed to crawl %s; status %d, message: %s, shacl status: %s, shacl message: %s",
		e.Url, e.Status, e.Message, e.ShaclStatus, e.ShaclErrorMessage)
}

// Crawl stats for a particular sitemap
type SitemapCrawlStats struct {
	Type              string          `json:"@type"`                     // schema.org type
	SitemapName       string          `json:"schema:name"`               // schema.org/name
	SuccessfulUrls    []string        `json:"schema:url"`                // schema.org/url (plural)
	CrawlFailures     []UrlCrawlError `json:"schema:FailedActionStatus"` // unmapped
	SecondsToComplete float64         `json:"schema:duration"`           // schema.org/duration (in seconds)
	SitesHarvested    int             `json:"numberOfSitesHarvested"`    // unmapped
	SitesInSitemap    int             `json:"numberOfSitesInSitemap"`    // unmapped
}

// A sitemap index is just a list of sitemaps
type SitemapIndexCrawlStats []SitemapCrawlStats

func (s SitemapIndexCrawlStats) GetJsonLdContext() map[string]any {
	output := map[string]any{
		"schema": "https://schema.org/",
		"http":   "https://www.w3.org/2011/http-statusCodes#",
		"sh":     "https://www.w3.org/ns/shacl#",
		"numberOfSitesHarvested": map[string]string{
			"@id":   "https://example.com/vocab#numberOfSitesHarvested",
			"@type": "http://www.w3.org/2001/XMLSchema#integer",
		},
		"numberOfSitesInSitemap": map[string]string{
			"@id":   "https://example.com/vocab#numberOfSitesInSitemap",
			"@type": "http://www.w3.org/2001/XMLSchema#integer",
		},
		"successfulUrls": map[string]string{
			"@id":   "https://example.com/vocab#successfulUrls",
			"@type": "http://www.w3.org/2001/XMLSchema#string",
		},
	}
	return output
}

// Serialize the sitemap index crawl stats to proper JSON-LD
func (s SitemapIndexCrawlStats) ToJsonLd() (string, error) {
	for i := range s {
		s[i].Type = "schema:DataFeedItem"
	}
	output := map[string]any{
		"@type":    "schema:DataFeed",
		"@graph":   s,
		"@context": s.GetJsonLdContext(),
	}

	data, err := json.Marshal(output)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s SitemapIndexCrawlStats) ToJsonLdReader() (io.Reader, error) {
	data, err := s.ToJsonLd()
	if err != nil {
		return nil, err
	}
	return strings.NewReader(data), nil
}
