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

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, storage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)

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

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, storage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)

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

func TestBulkSitemapWithShaclConnectionIssueDoesntCrash(t *testing.T) {
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

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, storage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)

	sitemap.URL[0].Loc = unique_id

	// this is intentionally a random invalid address to simulate a connection issue with the SHACL validator; we want to make sure this doesn't cause the harvest to fail since we want to be resilient to SHACL validator issues
	badGrpcClient, err := NewShaclGrpcClientFromAddr("0.0.0.0:1020202")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, badGrpcClient, false, false)
	require.NoError(t, err)

	stats, _, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	hasFiles, err := storage.ListDir("/summoned/test_sitemap/")
	require.NoError(t, err)
	require.Equal(t, len(hasFiles), 3, "There should be 3 files since all three sites should be harvested successfully even if there are SHACL validation issues")

	reader, err := storage.Get("/summoned/test_sitemap/aHR0cHM6Ly9hcGkud3dkaC5pbnRlcm5ldG9md2F0ZXIuYXBwL2NvbGxlY3Rpb25zL25vYWEtcmZjL2l0ZW1zL0FGUFUx.jsonld")
	require.NoError(t, err)
	dataAsStr, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(dataAsStr), `American Fork - American Fork  Nr  Up Pwrplnt  Abv`)

	require.Equal(t, 3, stats.WarningStats.TotalShaclFailures, "All 3 features should have had SHACL validation failures since the SHACL client couldn't connect, but this shouldn't cause the harvest to fail")
	require.Equal(t, len(stats.WarningStats.ShaclWarnings), stats.WarningStats.TotalShaclFailures)

	require.Equal(t, 3, stats.SuccessfulSites, "All 3 sites should be successful in strict shacl mode")
}
