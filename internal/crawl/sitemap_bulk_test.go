// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"testing"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestBulkSitemap(t *testing.T) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "./testdata/bulk_sitemap",
			Dockerfile: "Dockerfile",
			Tag:        "simple_bulk_test_container",
		},
	}
	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		// just build don't run
		Started: false,
	}
	_, err := testcontainers.GenericContainer(context.Background(), genericContainerReq)
	require.NoError(t, err)

	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/bulk": {
				StatusCode: 200,
				File:       "testdata/bulk_sitemap/sitemap.xml",
			},
		})

	storage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/bulk", 1, storage, "test")
	require.NoError(t, err)

	sitemap.isBulkSitemap = true

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, "", false, false)
	require.NoError(t, err)

	_, _, err = sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)
}
