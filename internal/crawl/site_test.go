// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"net/http"
	"testing"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/crawl/url_info"
	"github.com/internetofwater/nabu/internal/mainstems"
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
	report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:         mockedClient,
		storageDestination: &storage.DiscardCrawlStorage{},
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
	report, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:         mockedClient,
		storageDestination: &storage.DiscardCrawlStorage{},
	})
	require.NoError(t, err)
	require.Equal(t, 0, report.nonFatalError.Status)
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
	_, err := harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:         mockedClient,
		storageDestination: &storage.DiscardCrawlStorage{},
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
			validator, false)
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

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, false)

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

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, false)
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

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, validator, true)
		require.NoError(t, err)
		_, err = harvestOnePID(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.Error(t, err)
	})
}

func TestHarvestOnePIDAddsMainstem(t *testing.T) {
	// the source document for this gage does not reference a mainstem so it must come from the lookup
	const pid = "https://pids.geoconnex.dev/cdss/gages/BASMOUCO"
	url := url_info.NewUrlFromString(pid)
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		pid: {
			StatusCode:  200,
			File:        "../synchronizer/testdata/pids/BASMOUCO.jsonld",
			ContentType: "application/ld+json",
		},
		"https://pids.geoconnex.dev/robots.txt": {
			StatusCode:  200,
			File:        "../synchronizer/testdata/pids/robots.txt",
			ContentType: "text/plain",
		},
	})

	conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []url_info.URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 1}, nil, false)
	require.NoError(t, err)

	t.Run("no mainstem without a mainstem service", func(t *testing.T) {
		result, err := harvestOnePID(context.Background(), "cdss:co_gages__0", url, &conf)
		require.NoError(t, err)
		require.NotNil(t, result.feature)
		require.Empty(t, result.feature.MainstemURI)
		require.Equal(t, readFile(t, "../synchronizer/testdata/pids/BASMOUCO.jsonld"), result.feature.JSONLD, "the jsonld should be unchanged without enrichment")
	})

	t.Run("mainstem is set with a mainstem service", func(t *testing.T) {
		service, err := mainstems.NewS3FlatgeobufMainstemService("../synchronizer/testdata/colorado_subset.fgb")
		require.NoError(t, err)
		conf.mainstemService = service
		result, err := harvestOnePID(context.Background(), "cdss:co_gages__0", url, &conf)
		require.NoError(t, err)
		require.NotNil(t, result.feature)
		require.Equal(t, "https://geoconnex.us/ref/mainstems/2597141", result.feature.MainstemURI)
		require.Contains(t, string(result.feature.JSONLD), `"https://www.opengis.net/def/schema/hy_features/hyf/linearElement":{"@id":"https://geoconnex.us/ref/mainstems/2597141"}`, "the mainstem should be added to the jsonld")
		require.Equal(t, "https://pids.geoconnex.dev/cdss/gages/BASMOUCO", result.feature.ID, "the extracted columns should be unchanged")
	})
}

func TestSetMainstemServiceRequiresFile(t *testing.T) {
	getService := func() (mainstems.MainstemService, error) {
		t.Fatal("the mainstem service should not be created")
		return nil, nil
	}
	config := SitemapHarvestConfig{}

	require.NoError(t, SitemapIndex{}.setMainstemService(&config, SitemapMetadata{SitemapID: "a"}, getService), "sitemaps that do not request mainstems need no file")
	require.Nil(t, config.mainstemService)

	err := SitemapIndex{}.setMainstemService(&config, SitemapMetadata{SitemapID: "a", AddMainstems: true}, getService)
	require.ErrorContains(t, err, "but no mainstem file was provided")
}
