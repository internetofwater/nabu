// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
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

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://waterdata.usgs.gov/robots.txt": {File: "testdata/usgs_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
		"https://google.com/robots.txt":         {File: "testdata/google_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
		"https://geoconnex.us/robots.txt":       {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})

	for _, tc := range testCases {
		robotstxt, err := newRobots(mockedClient, tc.url)
		if tc.shouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			allowed := robotstxt.Test(common.HarvestAgent)
			assert.Equal(t, tc.allowsCrawling, allowed)
		}
	}

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

	t.Run("usgs jsonld", func(t *testing.T) {
		data, err := os.ReadFile("testdata/usgs_monitoring_location_351715080252401.html")
		require.NoError(t, err)
		jsonld, err := GetJsonLDFromHTML(data)
		require.NoError(t, err)
		require.JSONEq(t, jsonld, `{
	"@context": ["https://opengeospatial.github.io/ELFIE/json-ld/hyf.jsonld"],
	"@id": "http://waterdataui-production.wma.chs.usgs.gov/monitoring-location/USGS-351715080252401/",
	"@type": "http://www.opengeospatial.org/standards/waterml2/hy_features/HY_HydroLocation",
	"name": "ST-159",
	"description": "Discover water data collected at monitoring location USGS-351715080252401, located in North Carolina and find additional nearby monitoring locations.",
	"sameAs": "https://waterdata.usgs.gov/nwis/inventory/?site_no=351715080252401",
	"HY_HydroLocationType": "hydrometricStation",
	"geo": {
		"@type": "schema:GeoCoordinates",
		"latitude": "35.287642898996",
		"longitude": "-80.4231199054094"
	}
}`)
	})
}

func TestSitemapStatusTracker(t *testing.T) {

	tracker := NewSitemapStatusTracker(2)
	require.False(t, tracker.AppearsDown())
	tracker.AddSiteFailure()
	require.False(t, tracker.AppearsDown())
	tracker.AddSiteFailure()
	require.True(t, tracker.AppearsDown())
	tracker.AddSiteSuccess()
	require.False(t, tracker.AppearsDown())

	tracker2 := NewSitemapStatusTracker(0)
	require.False(t, tracker2.AppearsDown())
	tracker2.AddSiteFailure()
	require.False(t, tracker2.AppearsDown())
}
