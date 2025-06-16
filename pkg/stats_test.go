// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package pkg

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUrlCrawlError_Error(t *testing.T) {
	err := UrlCrawlError{
		Url:               "http://example.com",
		Status:            404,
		Message:           "not found",
		ShaclStatus:       ShaclSkipped,
		ShaclErrorMessage: "shape error",
	}
	msg := err.Error()
	assert.Contains(t, msg, "http://example.com")
	assert.Contains(t, msg, "404")
	assert.Contains(t, msg, "not found")
}

func TestUrlCrawlError_JSONSerialization(t *testing.T) {
	orig := UrlCrawlError{
		Url:               "http://example.com",
		Status:            500,
		Message:           "server error",
		ShaclStatus:       ShaclSkipped,
		ShaclErrorMessage: "no error",
	}
	data, err := json.Marshal(orig)
	require.NoError(t, err, "Marshal should not fail")

	var decoded UrlCrawlError
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err, "Unmarshal should not fail")
	assert.Equal(t, orig, decoded, "Decoded struct should match original")
}

func TestSitemapIndexCrawlStats_ToJson(t *testing.T) {
	stats := SitemapIndexCrawlStats{
		{
			CrawlFailures: []UrlCrawlError{
				{
					Url:               "http://fail.com",
					Status:            400,
					Message:           "bad request",
					ShaclStatus:       ShaclSkipped,
					ShaclErrorMessage: "shape fail",
				},
			},
			SecondsToComplete: 1.23,
			SitemapName:       "sitemap1.xml",
		},
	}
	jsonStr, err := stats.ToJson()
	require.NoError(t, err, "ToJson should not fail")

	var decoded SitemapIndexCrawlStats
	err = json.Unmarshal([]byte(jsonStr), &decoded)
	require.NoError(t, err, "Unmarshal should not fail")

	require.Len(t, decoded, 1, "Should decode one sitemap")
	require.Len(t, decoded[0].CrawlFailures, 1, "Should decode one crawl failure")
	assert.Equal(t, "http://fail.com", decoded[0].CrawlFailures[0].Url)
	assert.Equal(t, 400, decoded[0].CrawlFailures[0].Status)
	assert.Equal(t, "bad request", decoded[0].CrawlFailures[0].Message)
	assert.Equal(t, ShaclSkipped, decoded[0].CrawlFailures[0].ShaclStatus)
	assert.Equal(t, "shape fail", decoded[0].CrawlFailures[0].ShaclErrorMessage)
	assert.Equal(t, 1.23, decoded[0].SecondsToComplete)
	assert.Equal(t, "sitemap1.xml", decoded[0].SitemapName)
}

func TestSitemapIndexCrawlStats_Empty(t *testing.T) {
	stats := SitemapIndexCrawlStats{}
	jsonStr, err := stats.ToJson()
	require.NoError(t, err, "ToJson should not fail for empty stats")
	assert.Equal(t, "[]", jsonStr)

	var decoded SitemapIndexCrawlStats
	err = json.Unmarshal([]byte(jsonStr), &decoded)
	require.NoError(t, err, "Unmarshal should not fail for empty stats")
	assert.Empty(t, decoded)
}

func TestSitemapCrawlStats_NoFailures(t *testing.T) {
	stats := SitemapIndexCrawlStats{
		{
			CrawlFailures:     nil,
			SecondsToComplete: 0.5,
			SitemapName:       "empty.xml",
		},
	}
	jsonStr, err := stats.ToJson()
	require.NoError(t, err)

	var decoded SitemapIndexCrawlStats
	err = json.Unmarshal([]byte(jsonStr), &decoded)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	assert.Empty(t, decoded[0].CrawlFailures)
	assert.Equal(t, "empty.xml", decoded[0].SitemapName)
	assert.Equal(t, 0.5, decoded[0].SecondsToComplete)
}
