// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/internal/s2cells"
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
	return harvestTestSitemapWithMetadata(t, mocks, store, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml"})
}

func harvestTestSitemapWithMetadata(t *testing.T, mocks map[string]common.MockResponse, store storage.CrawlStorage, metadata SitemapMetadata) (pkg.SitemapCrawlStats, error) {
	mockedClient := common.NewMockedClient(true, mocks)
	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, store, metadata)
	require.NoError(t, err)
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
	require.NoError(t, err)
	return sitemap.Harvest(context.Background(), &config)
}

// read every feature in the parquet files for a sitemap
func readHarvestedFeatures(t *testing.T, store storage.CrawlStorage, sitemapId string) []parquettable.Feature {
	features, exists, err := readStoredFeatures(context.Background(), store, sitemapId)
	require.NoError(t, err)
	require.True(t, exists, "the parquet files for the sitemap should exist")
	return features
}

// the features whose JSON-LD is the contents of the given file; the url a feature was
// harvested from is not stored and some test documents do not have an @id, so the
// harvested JSON-LD is how a test identifies which mocked document a feature came from
func featuresFromFile(t *testing.T, features []parquettable.Feature, path string) []parquettable.Feature {
	expected := readFile(t, path)
	matching := []parquettable.Feature{}
	for _, feature := range features {
		if bytes.Equal(feature.JSONLD, expected) {
			matching = append(matching, feature)
		}
	}
	return matching
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

	for _, dir := range []string{"summoned/structured", "summoned/rdf"} {
		files, err := store.ListDir(dir)
		require.NoError(t, err)
		require.Len(t, files, 1, "there should only be one parquet file of each kind for the sitemap and no individual jsonld files")
	}

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 3)
	for _, mock := range sitemapMocks() {
		if mock.ContentType != "application/ld+json" {
			continue
		}
		require.Len(t, featuresFromFile(t, features, mock.File), 1, "the jsonld should be stored exactly as it was harvested")
	}
	require.Equal(t, "Eufaula", featuresFromFile(t, features, "testdata/reference_feature.jsonld")[0].Name)
}

func TestHarvestFallsBackToUrlWithoutAnId(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	_, err = harvestTestSitemap(t, sitemapMocks(), store)
	require.NoError(t, err)
	features := readHarvestedFeatures(t, store, "test")

	withId := featuresFromFile(t, features, "testdata/reference_feature.jsonld")
	require.Len(t, withId, 1)
	require.Equal(t, "https://geoconnex.us/ref/places/0124568", withId[0].ID, "the @id should be used when the document has one")

	withoutId := featuresFromFile(t, features, "testdata/reference_feature_2.jsonld")
	require.Len(t, withoutId, 1)
	require.Equal(t, "https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2", withoutId[0].ID, "documents without an @id should be identified by the url they were crawled from")
}

func TestHarvestAddsS2Cells(t *testing.T) {
	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	_, err = harvestTestSitemapWithMetadata(t, sitemapMocks(), store, SitemapMetadata{SitemapID: "test", Loc: "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", AddS2: true})
	require.NoError(t, err)

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 3)
	withGeometry := 0
	for _, feature := range features {
		if len(feature.Geometry) == 0 {
			require.NotContains(t, string(feature.JSONLD), s2cells.SpatiotemporalNamespace, "features without a geometry have no S2 cells")
			continue
		}
		withGeometry++
		relations, err := s2cells.CellRelationsForWkb(feature.Geometry)
		require.NoError(t, err)
		require.NotEmpty(t, relations)
		for _, relation := range relations {
			require.Contains(t, string(feature.JSONLD), s2cells.CellIRI(relation.Cell), "every S2 cell the geometry intersects should be in the jsonld")
			require.Contains(t, string(feature.JSONLD), relation.Relation)
		}
	}
	require.Positive(t, withGeometry)

	t.Run("cells are only added when the sitemap requests them", func(t *testing.T) {
		withoutS2, err := storage.NewLocalTempFSCrawlStorage()
		require.NoError(t, err)
		_, err = harvestTestSitemap(t, sitemapMocks(), withoutS2)
		require.NoError(t, err)
		for _, feature := range readHarvestedFeatures(t, withoutS2, "test") {
			require.NotContains(t, string(feature.JSONLD), s2cells.SpatiotemporalNamespace)
		}
	})
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

	// ensure that the stats are stored in the footer of the parquet file
	stats := readStoredCrawlStats(t, storage, "test")
	require.Equal(t, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", stats.SitemapSourceLink)
	require.Equal(t, "test", stats.SitemapName)
	require.Equal(t, 3, stats.SuccessfulSites)
	require.False(t, stats.DatasetDown)
}

func TestHarvestTwiceOverridesFile(t *testing.T) {
	const urlToHarvestDifferently = "https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"

	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	mocks := sitemapMocks()
	_, err = harvestTestSitemap(t, mocks, store)
	require.NoError(t, err)
	require.Len(t, featuresFromFile(t, readHarvestedFeatures(t, store, "test"), "testdata/reference_feature.jsonld"), 1)

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
	require.Empty(t, featuresFromFile(t, features, "testdata/reference_feature.jsonld"), "The document should have been replaced with the new content")
	require.Len(t, featuresFromFile(t, features, "testdata/reference_feature_2.jsonld"), 2, "both urls should now have the same content")
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
	require.Equal(t, 2, stats.TotalCrawlFailures)
	require.Equal(t, 2, readStoredCrawlStats(t, store, "test").TotalCrawlFailures, "the stats of the latest harvest should be in the footer")
	require.Equal(t, 3, stats.SitesInSitemap, "All 3 sites should be in the sitemap")

	features := readHarvestedFeatures(t, store, "test")
	require.Len(t, features, 1, "the parquet files should only contain the documents fetched in the most recent harvest")
	require.Len(t, featuresFromFile(t, features, sitemapMocks()["https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A"].File), 1)
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
		for _, path := range []string{SummonedStructuredParquetPath("test"), SummonedRDFParquetPath("test")} {
			exists, err := emptyStore.Exists(path)
			require.NoError(t, err)
			require.False(t, exists)
		}
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
	require.Len(t, featuresFromFile(t, features, sitemapMocks()["https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A"].File), 1)
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
