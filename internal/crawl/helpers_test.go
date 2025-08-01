// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBasename(t *testing.T) {
	testCases := []struct {
		url        string
		expected   string
		shouldFail bool
	}{
		{
			url:      "https://geoconnex.us/sitemap.xml",
			expected: "https://geoconnex.us",
		},
		{
			url:      "https://google.com",
			expected: "https://google.com",
		},
		{
			url:      "https://geoconnex.us/",
			expected: "https://geoconnex.us",
		},
	}

	for _, tc := range testCases {
		basename, err := getHostname(tc.url)
		if tc.shouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tc.expected, basename)
		}
	}

}

func TestRobots(t *testing.T) {
	testCases := []struct {
		url            string
		allowsCrawling bool
		shouldFail     bool
	}{
		{
			url:            "https://waterdata.usgs.gov/robots.txt",
			allowsCrawling: true,
		},
		{
			url:            "https://google.com",
			allowsCrawling: true,
		},
		{
			url:            "https://geoconnex.us/",
			allowsCrawling: true,
		},
		{
			url:            "https://geoconnex.us/usgs/monitoring-location/430208087543202",
			allowsCrawling: true,
		},
	}

	for _, tc := range testCases {
		robotstxt, err := newRobots(tc.url)
		if tc.shouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			allowed := robotstxt.Test(gleanerAgent)
			assert.Equal(t, tc.allowsCrawling, allowed)
		}
	}

}

func FuzzGetHash(f *testing.F) {
	// Seed with example inputs
	f.Add([]byte("test data"))
	f.Add([]byte("test data2"))

	f.Fuzz(func(t *testing.T, input []byte) {
		// Run function with fuzz input
		hash := generateHashFilename(input)

		// rehash to verify correctness
		expectedHash := fmt.Sprintf("%x.jsonld", sha256.Sum256(input))
		require.Equal(t, expectedHash, hash, "hash should match expected sha256")
	})
}

func TestGetJsonLdFromHTML(t *testing.T) {
	t.Run("end to end", func(t *testing.T) {
		data, err := os.ReadFile("testdata/html_with_jsonld.html")
		require.NoError(t, err)
		jsonld, err := GetJsonLDFromHTML(data)
		require.NoError(t, err)
		require.Contains(t, jsonld, "https://opengeospatial.github.io/ELFIE/contexts/elfie-2/hy_features.jsonld")

		processor, options, err := common.NewJsonldProcessor(true, make(map[string]string))
		require.NoError(t, err)
		_, err = common.JsonldToNQ(jsonld, processor, options)
		require.NoError(t, err)
	})
	t.Run("mislabeled jsonld script tag", func(t *testing.T) {
		data, err := os.ReadFile("testdata/html_without_jsonld.html")
		require.NoError(t, err)
		_, err = GetJsonLDFromHTML(data)
		require.Error(t, err)
	})
}
