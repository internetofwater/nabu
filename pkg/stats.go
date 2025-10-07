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
}

func (e UrlCrawlError) IsNil() bool {
	return e.Url == "" && e.Status == 0 && e.Message == ""
}

// A warning for a particular URL in a sitemap
type ShaclInfo struct {
	// THe url against which shacl validation was run
	Url string
	// the status of the shacl validation operation itself
	ShaclStatus ShaclStatus
	// the shacl validation message
	ShaclValidationMessage string
}

func (e ShaclInfo) IsNil() bool {
	return e.ShaclStatus == "" && e.ShaclValidationMessage == ""
}

func (e ShaclInfo) Error() string {
	return fmt.Errorf("shacl validation was %s: %s", e.ShaclStatus, e.ShaclValidationMessage).Error()
}

// Return a string representation of the error
func (e UrlCrawlError) Error() string {
	return fmt.Errorf("failed to crawl %s; status %d, message: %s",
		e.Url, e.Status, e.Message).Error()
}

type WarningReport struct {
	// The number of sites that had warnings
	// This may be much greater than the data
	// contained in CrawlWarnings given the fact that
	// we may abbreviate our shacl messages to reduce
	// verbosity
	TotalShaclFailures int
	// Warnings about sites that were harvested
	ShaclWarnings []ShaclInfo
}

// Crawl stats for a particular sitemap
type SitemapCrawlStats struct {
	// The link to the sitemap itself, containing all
	// the sites that were harvested
	// This allows a client to inspect the sitemap
	// without needing to keep it all in this payload
	SitemapSourceLink string
	// Metadata about why a sitemap failed to be harvested
	CrawlFailures []UrlCrawlError
	// Metadata about shacl validation warnings
	WarningStats WarningReport
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
