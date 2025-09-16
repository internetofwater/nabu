// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"testing"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"

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

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml")
	require.NoError(t, err)
	_, errs := sitemap.
		SetStorageDestination(storage.DiscardCrawlStorage{}).
		Harvest(context.Background(), mockedClient, 10, "test", "", false, true)
	require.NoError(t, errs)
}
