// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/crawl/url_info"
	"github.com/internetofwater/nabu/pkg"
	"github.com/stretchr/testify/require"
)

func TestGetJsonLDWithBadMimetype(t *testing.T) {

	resp := &http.Response{}
	resp.Header = http.Header{
		"Content-Type": []string{"text/DUMMY"},
	}
	_, err := getJSONLD(resp, url_info.URL{}, nil)
	require.ErrorAs(t, err, &pkg.UrlCrawlError{})

}

func TestNoJsonLDInHTML(t *testing.T) {

	resp := &http.Response{}
	resp.Header = http.Header{
		"Content-Type": []string{"text/html"},
	}
	_, err := getJSONLD(resp, url_info.URL{}, nil)
	require.ErrorAs(t, err, &pkg.UrlCrawlError{})

}

func TestNoJSONLDInHarvest(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			Body:        "Nothing to parse",
			ContentType: "text/html",
			StatusCode:  200,
		},
	})

	url := url_info.NewUrlFromString(dummy_domain)
	check := atomic.Bool{}
	check.Store(true)
	report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
	require.Equal(t, 200, report.nonFatalError.Status)
}

func TestTimeout(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			File:    "testdata/reference_feature.jsonld",
			Timeout: true,
		},
	})

	url := url_info.NewUrlFromString(dummy_domain)
	check := atomic.Bool{}
	check.Store(false)
	report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
	require.Equal(t, 0, report.nonFatalError.Status)
	require.Contains(t, report.nonFatalError.Message, "timeout")
}

func TestTimeoutWithHEADRequest(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			File:    "testdata/reference_feature.jsonld",
			Timeout: true,
		},
	})

	url := url_info.NewUrlFromString(dummy_domain)
	check := atomic.Bool{}
	check.Store(true)
	report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
	require.Equal(t, 0, report.nonFatalError.Status)
	require.Contains(t, report.nonFatalError.Message, "Head")
	require.Contains(t, report.nonFatalError.Message, "timeout")
}

func TestHarvestOneSite(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			StatusCode:  200,
			File:        "testdata/reference_feature.jsonld",
			ContentType: "application/ld+json",
		},
	})

	url := url_info.NewUrlFromString(dummy_domain)
	check := atomic.Bool{}
	check.Store(true)
	_, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
}

func TestHarvestWithShaclValidation(t *testing.T) {
	validator, err := NewLocalShaclValidator()
	require.NoError(t, err)

	t.Run("valid jsonld", func(t *testing.T) {
		const dummy_domain = "http://google.com"

		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/reference_feature.jsonld",
				ContentType: "application/ld+json",
			},
			dummy_domain + "/robots.txt": {
				StatusCode:  200,
				File:        "testdata/google_robots.txt",
				ContentType: "application/text/plain",
			},
		})

		url := url_info.NewUrlFromString(dummy_domain)
		sitemap := Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}

		conf, err := NewSitemapHarvestConfig(mockedClient,
			&sitemap,
			validator, false, false)
		require.NoError(t, err)
		_, err = harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
	})
	t.Run("empty jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"

		url := url_info.NewUrlFromString(dummy_domain)
		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode: 200,
				File:       "testdata/emptyAsTriples.jsonld",
			},
			dummy_domain + "/robots.txt": {
				StatusCode:  200,
				File:        "testdata/usgs_robots.txt",
				ContentType: "application/text/plain",
			},
		})

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, false, false)

		require.NoError(t, err)
		report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		require.Empty(t, report.warning.ShaclStatus)
	})
	t.Run("nonconforming jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"
		url := url_info.NewUrlFromString(dummy_domain)
		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/nonconforming.jsonld",
				ContentType: "application/ld+json",
			},
			dummy_domain + "/robots.txt": {
				StatusCode:  200,
				File:        "testdata/usgs_robots.txt",
				ContentType: "application/text/plain",
			},
		})

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, false, false)
		require.NoError(t, err)
		report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		require.Equal(t, pkg.ShaclInvalid, report.warning.ShaclStatus)
	})

	t.Run("strict mode exits early on bad jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"
		url := url_info.NewUrlFromString(dummy_domain)
		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/nonconforming.jsonld",
				ContentType: "application/ld+json",
			},
			dummy_domain + "/robots.txt": {
				StatusCode:  200,
				File:        "testdata/usgs_robots.txt",
				ContentType: "application/text/plain",
			},
		})

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, true, true)
		require.NoError(t, err)
		_, err = harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.Error(t, err)
	})
}
