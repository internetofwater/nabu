// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestBulkSitemap(t *testing.T) {
	unique_id := uuid.New().String()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "./testdata/bulk_sitemap",
			Dockerfile: "Dockerfile",
			Repo:       unique_id,
			Tag:        "latest",
			KeepImage:  true,
		},
	}
	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		// just build don't run
		Started: false,
	}
	container, err := testcontainers.GenericContainer(context.Background(), genericContainerReq)
	require.NoError(t, err)

	defer func() {
		_ = container.Terminate(context.Background())
	}()

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

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/bulk", 1, storage, "test_sitemap")
	require.NoError(t, err)

	sitemap.isBulkSitemap = true
	sitemap.URL[0].Loc = unique_id

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, &mockShaclValidatorClient{}, false, false)
	require.NoError(t, err)

	stats, _, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.Equal(t, 3, stats.SitesInSitemap)
	hasFiles, err := storage.ListDir("/summoned/test_sitemap/")
	require.NoError(t, err)
	require.Equal(t, len(hasFiles), 3)

	reader, err := storage.Get("/summoned/test_sitemap/aHR0cHM6Ly9hcGkud3dkaC5pbnRlcm5ldG9md2F0ZXIuYXBwL2NvbGxlY3Rpb25zL25vYWEtcmZjL2l0ZW1zL0FGUFUx.jsonld")
	require.NoError(t, err, "Failed to get the data; the id for the jsonld should be stable and consistent")
	dataAsStr, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(dataAsStr), `American Fork - American Fork  Nr  Up Pwrplnt  Abv`)

	require.Equal(t, 1, stats.WarningStats.TotalShaclFailures)
	require.Equal(t, len(stats.WarningStats.ShaclWarnings), stats.WarningStats.TotalShaclFailures)

	require.Equal(t, 3, stats.SuccessfulSites, "all three sites should be successful; even if there is a shacl error, the site itself is still harvested")
}

func TestBulkSitemapWithStrictShaclMode(t *testing.T) {
	unique_id := uuid.New().String()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "./testdata/bulk_sitemap",
			Dockerfile: "Dockerfile",
			Repo:       unique_id,
			Tag:        "latest",
			KeepImage:  true,
		},
	}
	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		// just build don't run
		Started: false,
	}
	container, err := testcontainers.GenericContainer(context.Background(), genericContainerReq)
	require.NoError(t, err)

	defer func() {
		_ = container.Terminate(context.Background())
	}()

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

	sitemap, err := NewSitemap(context.Background(), mockedClient, "https://geoconnex.us/sitemap/iow/bulk", 1, storage, "test_sitemap")
	require.NoError(t, err)

	sitemap.isBulkSitemap = true
	sitemap.URL[0].Loc = unique_id

	const STRICT_SHACL_MODE = true
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, &mockShaclValidatorClient{}, STRICT_SHACL_MODE, false)
	require.NoError(t, err)

	stats, _, err := sitemap.
		Harvest(context.Background(), &config)
	require.ErrorContains(t, err, "with shacl failure invalid jsonld content")

	hasFiles, err := storage.ListDir("/summoned/test_sitemap/")
	require.NoError(t, err)
	require.Equal(t, len(hasFiles), 1)

	reader, err := storage.Get("/summoned/test_sitemap/aHR0cHM6Ly9hcGkud3dkaC5pbnRlcm5ldG9md2F0ZXIuYXBwL2NvbGxlY3Rpb25zL25vYWEtcmZjL2l0ZW1zL0FGUFUx.jsonld")
	require.NoError(t, err)
	dataAsStr, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(dataAsStr), `American Fork - American Fork  Nr  Up Pwrplnt  Abv`)

	require.Equal(t, 1, stats.WarningStats.TotalShaclFailures)
	require.Equal(t, len(stats.WarningStats.ShaclWarnings), stats.WarningStats.TotalShaclFailures)

	require.Equal(t, 1, stats.SuccessfulSites, "only one site should be successful in strict shacl mode")
}
