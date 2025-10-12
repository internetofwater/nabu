// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/crawl/url_info"
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

func TestNamesAreBase64(t *testing.T) {
	mocks := map[string]common.MockResponse{
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
	mockedClient := common.NewMockedClient(
		true, mocks,
	)

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, false)
	require.NoError(t, err)

	_, _, err = sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	const root = ""
	storageItems, err := storage.ListDir(root)
	require.NoError(t, err)
	require.Equal(t, len(storageItems), 2, "There are 3 mocks so there is a list of length 2. Thus there should also be a storage items of list length 2")
	for item := range mocks {
		if strings.HasSuffix(item, ".jsonld") {
			url := url_info.NewUrlFromString(item)
			path, err := urlToStoragePath(sitemap.sitemapId, url)
			require.NoError(t, err)
			require.Contains(t, storageItems, path)
		}
	}
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

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, &storage.DiscardCrawlStorage{}, "test")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, false)
	require.NoError(t, err)

	results, _, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.Equal(t, results.SuccessfulSites, 3)
}

func TestHarvestTwiceOverridesFile(t *testing.T) {

	const urlToHarvestDifferently = "https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"
	mocks := map[string]common.MockResponse{
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap.xml",
		},
		urlToHarvestDifferently: {
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
	mockedClient := common.NewMockedClient(
		true,
		mocks,
	)

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, false)
	require.NoError(t, err)

	_, _, err = sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	pathInStorage, err := urlToStoragePath(sitemap.sitemapId, url_info.NewUrlFromString(urlToHarvestDifferently))
	require.NoError(t, err)
	dataInStorage, err := storage.Get(pathInStorage)
	require.NoError(t, err)
	dataInStorageAsBytes, err := io.ReadAll(dataInStorage)
	require.NoError(t, err)

	mockedContent, err := os.Open("testdata/reference_feature.jsonld")
	require.NoError(t, err)
	mockedContentAsBytes, err := io.ReadAll(mockedContent)
	require.NoError(t, err)
	require.Equal(t, dataInStorageAsBytes, mockedContentAsBytes)

	/*
		Once we have assured that the file is in the storage, we try
		harvesting it again but this time with a different content
		to make sure the the file is overwritten
	*/

	mocks[urlToHarvestDifferently] = common.MockResponse{
		StatusCode: 200,
		// new content
		File:        "testdata/reference_feature_2.jsonld",
		ContentType: "application/ld+json",
	}
	mockClientWithReplacedContent := common.NewMockedClient(true, mocks)
	sitemap, err = NewSitemap(context.Background(), mockClientWithReplacedContent, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)
	stats, _, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.Equal(t, stats.SuccessfulSites, 3)
	require.Len(t, stats.CrawlFailures, 0, "If we harvest the same content again and there is no bad status code, there should be no failures")

	dataInStorage, err = storage.Get(pathInStorage)
	require.NoError(t, err)
	dataInStorageAsBytes, err = io.ReadAll(dataInStorage)
	require.NoError(t, err)
	mockedContent, err = os.Open("testdata/reference_feature_2.jsonld")
	require.NoError(t, err)
	mockedContentAsBytes, err = io.ReadAll(mockedContent)
	require.NoError(t, err)
	require.Equal(t, dataInStorageAsBytes, mockedContentAsBytes, "The file should have been overwritten and contain the new content")
}

func TestHarvestSitemapWithCleanup(t *testing.T) {

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
	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, true)
	require.NoError(t, err)

	// store three files in the storage that are not part of the sitemap
	// thus we want these to be cleaned up
	err = storage.StoreWithoutServersideHash("summoned/"+sitemap.sitemapId+"/testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	err = storage.StoreWithoutServersideHash("summoned/"+sitemap.sitemapId+"/testfile2.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	err = storage.StoreWithoutServersideHash("summoned/"+sitemap.sitemapId+"/testfile3.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)

	stats, cleanupChan, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)
	// wait for cleanup to finish
	cleanedUpFiles := <-cleanupChan
	require.Len(t, cleanedUpFiles, 3, "THere should be 3 files cleaned up, representing the 3 testfiles.txt that were manually added")

	require.Len(t, stats.CrawlFailures, 0)
	require.Equal(t, stats.SuccessfulSites, 3)
	require.Len(t, stats.WarningStats.ShaclWarnings, 0)

	mockedClientWithErrors := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
				StatusCode: 200,
				File:       "testdata/sitemap.xml",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C": {
				StatusCode:  404,
				File:        "testdata/reference_feature.jsonld",
				ContentType: "application/ld+json",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
				StatusCode:  404,
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

	// store a file that is not in the sitemap
	err = storage.StoreWithoutServersideHash("summoned/"+sitemap.sitemapId+"/dummy.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)

	sitemap, err = NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)

	config, err = NewSitemapHarvestConfig(mockedClientWithErrors, sitemap, "", false, true)
	require.NoError(t, err)

	stats, cleanupChan, err = sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)
	// wait for cleanup to finish
	cleanedUpFiles = <-cleanupChan
	require.Len(t, cleanedUpFiles, 1, "There should be 1 file cleaned up, representing dummy.txt; the other sites which 404d but are in the sitemap should stay in case they are temporarily failing")

	require.Len(t, stats.CrawlFailures, 2, "Two sites had 404 errors")
	require.Equal(t, stats.SuccessfulSites, 1, "Only one site had a successful response")
	require.Len(t, stats.WarningStats.ShaclWarnings, 0)
	require.Equal(t, 3, stats.SitesInSitemap, "All 3 sites should be in the sitemap")
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
