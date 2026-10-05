// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/pkg"
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

// the default mocks for the test sitemap with three sites
func sitemapMocks() map[string]common.MockResponse {
	return map[string]common.MockResponse{
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap.xml",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C": {
			StatusCode:  200,
			File:        "testdata/reference_feature.jsonld",
			ContentType: "application/ld+json",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
			StatusCode:  200,
			File:        "testdata/reference_feature_2.jsonld",
			ContentType: "application/ld+json",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A": {
			StatusCode:  200,
			File:        "testdata/reference_feature_3.jsonld",
			ContentType: "application/ld+json",
		},
		"https://geoconnex.us/robots.txt": {
			StatusCode:  200,
			File:        "testdata/geoconnex_robots.txt",
			ContentType: "application/text/plain",
		},
	}
}

// harvest the test sitemap with the given mocks into the storage
func harvestTestSitemap(t *testing.T, mocks map[string]common.MockResponse, store storage.CrawlStorage) (pkg.SitemapCrawlStats, error) {
	mockedClient := common.NewMockedClient(true, mocks)
	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, store, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"})
	require.NoError(t, err)
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
	require.NoError(t, err)
	return sitemap.Harvest(context.Background(), &config)
}

// read every feature in the parquet file for a sitemap keyed by the url it was harvested from
func readHarvestedFeatures(t *testing.T, store storage.CrawlStorage, sitemapId string) map[string]parquettable.Feature {
	features := map[string]parquettable.Feature{}
	exists, err := readStoredFeatures(context.Background(), store, SummonedParquetPath(sitemapId), func(f parquettable.Feature) error {
		require.NotContains(t, features, f.URL, "each url should only be in the parquet file once")
		features[f.URL] = f
		return nil
	})
	require.NoError(t, err)
	require.True(t, exists, "the parquet file for the sitemap should exist")
	return features
}

func readFile(t *testing.T, path string) []byte {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func TestHarvestWritesOneParquetFile(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	_, err = harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)

	summoned, err := store.ListDir("summoned")
	require.NoError(t, err)
	require.Len(t, summoned, 1, "there should only be one parquet file for the sitemap and no individual jsonld files")

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 3)
	for url, mock := range sitemapMocks() {
		if mock.ContentType != "application/ld+json" {
			continue
		}
		require.Contains(t, features, url)
		require.Equal(t, readFile(t, mock.File), features[url].JSONLD, "the jsonld should be stored exactly as it was harvested")
	}
	require.Equal(t, "Eufaula", features["https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"].Name)
}

func TestHarvestSitemap(t *testing.T) {

	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
				StatusCode: 200,
				File:       "testdata/sitemap.xml",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C": {
				StatusCode:  200,
				File:        "testdata/reference_feature.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
				StatusCode:  200,
				File:        "testdata/reference_feature_2.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A": {
				StatusCode:  200,
				File:        "testdata/reference_feature_3.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/robots.txt": {
				StatusCode:  200,
				File:        "testdata/geoconnex_robots.txt",
				ContentType: "application/text/plain",
			},
		})

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, storage, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"})
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
	require.NoError(t, err)

	results, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.Equal(t, results.SuccessfulSites, 3)

	// ensure that the stats are uploaded to the storage dir
	data, err := storage.Get("metadata/sitemaps/test.json")
	require.NoError(t, err)
	statsAsBytes, err := io.ReadAll(data)
	require.NoError(t, err)
	var statsAsJson map[string]interface{}
	err = json.Unmarshal(statsAsBytes, &statsAsJson)
	require.NoError(t, err)
	require.Equal(t, statsAsJson["SitemapSourceLink"], "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml")
	require.Equal(t, statsAsJson["SitemapName"], "test")
	require.Equal(t, statsAsJson["DatasetDown"], false)
}

func TestHarvestTwiceOverridesFile(t *testing.T) {
	const urlToHarvestDifferently = "https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"

	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	mocks := sitemapMocks()
	_, err = harvestTestSitemap(t, mocks, store)
	require.NoError(t, err)
	require.Equal(t, readFile(t, "testdata/reference_feature.jsonld"), readHarvestedFeatures(t, store, "test")[urlToHarvestDifferently].JSONLD)

	// harvest again with different content to make sure the document is replaced
	mocks[urlToHarvestDifferently] = common.MockResponse{
		StatusCode:  200,
		File:        "testdata/reference_feature_2.jsonld",
		ContentType: "application/ld+json",
	}
	stats, err := harvestTestSitemap(t, mocks, store)
	require.NoError(t, err)

	require.Equal(t, stats.SuccessfulSites, 3)
	require.Len(t, stats.CrawlFailures, 0, "If we harvest the same content again and there is no bad status code, there should be no failures")

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 3)
	require.Equal(t, readFile(t, "testdata/reference_feature_2.jsonld"), features[urlToHarvestDifferently].JSONLD, "The document should have been replaced with the new content")
}

func TestHarvestLeavesOutDocumentsForFailingSites(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	stats, err := harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)
	require.Len(t, stats.CrawlFailures, 0)
	require.Equal(t, stats.SuccessfulSites, 3)
	require.Len(t, stats.WarningStats.ShaclWarnings, 0)

	mocksWithErrors := sitemapMocks()
	for _, url := range []string{"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C", "https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2"} {
		mock := mocksWithErrors[url]
		mock.StatusCode = 404
		mocksWithErrors[url] = mock
	}

	stats, err = harvestTestSitemap(t, mocksWithErrors, store)
	require.NoError(t, err)
	require.Len(t, stats.CrawlFailures, 2, "Two sites had 404 errors")
	require.Equal(t, stats.SuccessfulSites, 1, "Only one site had a successful response")
	require.Equal(t, 3, stats.SitesInSitemap, "All 3 sites should be in the sitemap")

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 1, "the parquet file should only contain the documents fetched in the most recent harvest")
	require.Contains(t, features, "https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A")
}

func TestHarvestKeepsPreviousFileIfNoDocumentsSucceed(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	_, err = harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)
	previousHarvest := readHarvestedFeatures(t, store, "test")

	// every url fails but there are fewer failures than are needed to assume the sitemap is down
	allFailing := sitemapMocks()
	for url, mock := range allFailing {
		if mock.ContentType == "application/ld+json" {
			mock.StatusCode = 404
			allFailing[url] = mock
		}
	}
	stats, err := harvestTestSitemap(t, allFailing, store)
	require.NoError(t, err, "failing urls are not fatal")
	require.Len(t, stats.CrawlFailures, 3)
	require.Zero(t, stats.SuccessfulSites)
	require.Equal(t, previousHarvest, readHarvestedFeatures(t, store, "test"), "an empty harvest should not replace the previous file")

	t.Run("no file is created if the first harvest has no documents", func(t *testing.T) {
		emptyStore, err := storage.NewLocalTempFSCrawlStorage()
		require.NoError(t, err)
		_, err = harvestTestSitemap(t, allFailing, emptyStore)
		require.NoError(t, err)
		exists, err := emptyStore.Exists(SummonedParquetPath("test"))
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestHarvestRemovesDocumentsNoLongerInSitemap(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	_, err = harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)

	smallerSitemap := filepath.Join(t.TempDir(), "sitemap.xml")
	err = os.WriteFile(smallerSitemap, []byte(`<?xml version='1.0' encoding='utf-8'?>
<ns0:urlset xmlns:ns0="http://www.sitemaps.org/schemas/sitemap/0.9">
<url><loc>https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A</loc></url>
</ns0:urlset>`), 0644)
	require.NoError(t, err)
	mocks := sitemapMocks()
	mocks["https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"] = common.MockResponse{StatusCode: 200, File: smallerSitemap}

	_, err = harvestTestSitemap(t, mocks, store)
	require.NoError(t, err)

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 1)
	require.Contains(t, features, "https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A")
}

func TestErrorGroupCtxCancelling(t *testing.T) {
	start := time.Now()

	group, ctx := errgroup.WithContext(context.Background())

	// Goroutine that simulates long work but will be cancelled
	group.Go(func() error {
		select {
		case <-time.After(10 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	group.Go(func() error {
		return errors.New("force cancel")
	})

	err := group.Wait()

	require.Error(t, err)
	require.Contains(t, err.Error(), "force cancel")

	require.Less(t, time.Since(start), 2*time.Second, "test took too long, context wasn't cancelled promptly")
}

func TestHarvestSitemapThatIsDown(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	_, err = harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)
	previousHarvest := readHarvestedFeatures(t, store, "test")

	mocks := sitemapMocks()
	for url, mock := range mocks {
		if mock.ContentType == "application/ld+json" {
			mock.StatusCode = 500
			mocks[url] = mock
		}
	}
	mockedClient := common.NewMockedClient(true, mocks)

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, store, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"})
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
	require.NoError(t, err)

	config.failedSitesToAssumeDatasetDown = 1

	stats, err := sitemap.
		Harvest(context.Background(), &config)
	var downErr *SitemapAppearsDownError
	require.ErrorAs(t, err, &downErr)

	// Although there are three sites in the sitemap
	// we should have only seen one failure before exiting
	require.Len(t, stats.CrawlFailures, 1)
	require.Equal(t, stats.SuccessfulSites, 0)
	require.Equal(t, stats.SitesInSitemap, 3)

	require.Equal(t, previousHarvest, readHarvestedFeatures(t, store, "test"), "The previous harvest should be kept as is since the sitemap was down; prompting an early exit")
}

func TestShaclConnectionIssueDoesntCauseFailure(t *testing.T) {

	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
				StatusCode: 200,
				File:       "testdata/sitemap.xml",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C": {
				StatusCode:  200,
				File:        "testdata/reference_feature.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
				StatusCode:  200,
				File:        "testdata/reference_feature_2.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A": {
				StatusCode:  200,
				File:        "testdata/reference_feature_3.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/robots.txt": {
				StatusCode:  200,
				File:        "testdata/geoconnex_robots.txt",
				ContentType: "application/text/plain",
			},
		})

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	// this is intentionally a random invalid address to simulate a connection issue with the SHACL validator; we want to make sure this doesn't cause the harvest to fail since we want to be resilient to SHACL validator issues
	badGrpcClient, err := NewGrpcShaclValidator("0.0.0.0:1020202")
	require.NoError(t, err)

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, storage, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"})
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, badGrpcClient, false)
	require.NoError(t, err)

	stats, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	// Although there are three sites in the sitemap
	// we should have only seen one failure before exiting
	require.Len(t, stats.CrawlFailures, 0)
	require.Equal(t, stats.SuccessfulSites, 3)
	require.Equal(t, stats.SitesInSitemap, 3)
	require.Equal(t, stats.WarningStats.TotalShaclFailures, 3, "All three features should have had SHACL validation failures since the SHACL client couldn't connect, but this shouldn't cause the harvest to fail")
}
