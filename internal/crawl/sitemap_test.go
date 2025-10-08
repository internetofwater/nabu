// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

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
		})

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, true)
	require.NoError(t, err)

	err = storage.Store("summoned/"+sitemap.sitemapId+"/testfile.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	err = storage.Store("summoned/"+sitemap.sitemapId+"/testfile2.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)
	err = storage.Store("summoned/"+sitemap.sitemapId+"/testfile3.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)

	_, cleanupChan, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)
	// wait for cleanup to finish
	cleanedUpFiles := <-cleanupChan
	require.Len(t, cleanedUpFiles, 3, "THere should be 3 files cleaned up, representing the 3 testfiles.txt that were manually added")

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
		})

	err = storage.Store("summoned/"+sitemap.sitemapId+"/dummy.txt", bytes.NewReader([]byte("dummy_data")))
	require.NoError(t, err)

	sitemap, err = NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 1, storage, "test")
	require.NoError(t, err)

	config, err = NewSitemapHarvestConfig(mockedClientWithErrors, sitemap, "", false, true)
	require.NoError(t, err)

	_, cleanupChan, err = sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)
	// wait for cleanup to finish
	cleanedUpFiles = <-cleanupChan
	require.Len(t, cleanedUpFiles, 3, "There should be 3 files cleaned up, representing dummy.txt that were manually added as well as the two features that were not found")
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
