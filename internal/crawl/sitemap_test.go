// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
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
				StatusCode: 200,
				File:       "testdata/reference_feature.jsonld",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
				StatusCode: 200,
				File:       "testdata/reference_feature.jsonld",
			},
			"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A": {
				StatusCode: 200,
				File:       "testdata/reference_feature.jsonld",
			},
		})

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 10, &storage.DiscardCrawlStorage{}, "test")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, false)
	require.NoError(t, err)

	_, errs := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, errs)
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
