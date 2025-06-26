// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// The status of the shacl validation
type ShaclStatus string

const (
	// Shacl validation was skipped and was not run
	ShaclSkipped ShaclStatus = "skipped"
	// The triples passed into shacl validation were invalid
	ShaclInvalid ShaclStatus = "invalid"
	// The triples passed into shacl validation were valid
	ShaclValid ShaclStatus = "valid"
)

// An error for a particular URL in a sitemap
type UrlCrawlError struct {
	// The URL that failed
	Url string
	// The http status code of the fetched Url
	Status int
	// a natural language error message describing the error
	Message string
	// the status of the shacl validation operation itself
	ShaclStatus ShaclStatus
	// the shacl validation message
	ShaclErrorMessage string
}

// Return a string representation of the error
func (e UrlCrawlError) Error() string {
	return fmt.Errorf("failed to crawl %s; status %d, message: %s, shacl status: %s, shacl message: %s",
		e.Url, e.Status, e.Message, e.ShaclStatus, e.ShaclErrorMessage).Error()
}

// Crawl stats for a particular sitemap
type SitemapCrawlStats struct {
	// Metadata about why a sitemap failed to be harvested
	CrawlFailures []UrlCrawlError
	// The number of seconds it took to crawl the sitemap
	SecondsToComplete float64
	// The name of the sitemap in the sitemap index
	SitemapName string
	// The number of sites that were successfully crawled and stored
	SitesHarvested int
	// The number of total sites in the sitemap
	SitesInSitemap int
}

// Serialize the sitemap crawl stats to json
// and return the result as an io.Reader
func (s SitemapCrawlStats) ToJsonIoReader() (io.Reader, error) {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(s)
	return buf, err
}

// A sitemap index is just a list of sitemaps and thus
// its status is just the status of each sitemap
type SitemapIndexCrawlStats []SitemapCrawlStats

// Serialize the sitemap index crawl stats to json
func (s SitemapIndexCrawlStats) ToJson() (string, error) {
	if data, err := json.Marshal(s); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}
