// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/pkg"

	"github.com/google/uuid"
	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/protoBuild"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"google.golang.org/grpc"
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

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, NewGrpcShaclValidatorFromClients(&mockShaclValidatorClient{}), false)
	require.NoError(t, err)

	stats, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.Equal(t, 3, stats.SitesInSitemap)
	features := readBulkFeatures(t, storage, "test_sitemap")
	require.Len(t, features, 3)

	feature := features["https://api.wwdh.internetofwater.app/collections/noaa-rfc/items/AFPU1"]
	require.Contains(t, string(feature.JSONLD), `American Fork - American Fork  Nr  Up Pwrplnt  Abv`)
	require.Equal(t, `American Fork - American Fork  Nr  Up Pwrplnt  Abv`, feature.Name)
	require.NotEmpty(t, feature.Geometry)
	require.Equal(t, unique_id, feature.URL, "the url of a bulk document is the container it came from")

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
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, NewGrpcShaclValidatorFromClients(&mockShaclValidatorClient{}), STRICT_SHACL_MODE)
	require.NoError(t, err)

	stats, err := sitemap.
		Harvest(context.Background(), &config)
	require.ErrorContains(t, err, "with shacl failure invalid jsonld content")

	exists, err := storage.Exists(SummonedParquetPath("test_sitemap"))
	require.NoError(t, err)
	require.False(t, exists, "a partial harvest should never be stored")

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
	badGrpcClient, err := NewGrpcShaclValidator("0.0.0.0:1020202")
	require.NoError(t, err)

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, badGrpcClient, false)
	require.NoError(t, err)

	stats, err := sitemap.
		Harvest(context.Background(), &config)
	require.NoError(t, err)

	features := readBulkFeatures(t, storage, "test_sitemap")
	require.Len(t, features, 3, "There should be 3 documents since all three sites should be harvested successfully even if there are SHACL validation issues")
	require.Contains(t, string(features["https://api.wwdh.internetofwater.app/collections/noaa-rfc/items/AFPU1"].JSONLD), `American Fork - American Fork  Nr  Up Pwrplnt  Abv`)

	require.Equal(t, 3, stats.WarningStats.TotalShaclFailures, "All 3 features should have had SHACL validation failures since the SHACL client couldn't connect, but this shouldn't cause the harvest to fail")
	require.Equal(t, len(stats.WarningStats.ShaclWarnings), stats.WarningStats.TotalShaclFailures)

	require.Equal(t, 3, stats.SuccessfulSites, "All 3 sites should be successful in strict shacl mode")
}

// build the bulk container in contextDir and return its image name
func buildBulkTestImage(t *testing.T, contextDir string) string {
	unique_id := uuid.New().String()
	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    contextDir,
				Dockerfile: "Dockerfile",
				Repo:       unique_id,
				Tag:        "latest",
				KeepImage:  true,
			},
		},
		// just build don't run
		Started: false,
	}
	container, err := testcontainers.GenericContainer(context.Background(), genericContainerReq)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})
	return unique_id
}

// read every feature in the parquet file for a sitemap keyed by @id
func readBulkFeatures(t *testing.T, store storage.CrawlStorage, sitemapId string) map[string]parquettable.Feature {
	features := map[string]parquettable.Feature{}
	exists, err := readStoredFeatures(context.Background(), store, SummonedParquetPath(sitemapId), func(f parquettable.Feature) error {
		require.NotContains(t, features, f.ID, "each @id should only be in the parquet file once")
		features[f.ID] = f
		return nil
	})
	require.NoError(t, err)
	require.True(t, exists, "the parquet file for the sitemap should exist")
	return features
}

// build a bulk container image that outputs the given lines
func buildBulkTestImageWithData(t *testing.T, lines []string) string {
	contextDir := t.TempDir()
	dockerfile, err := os.ReadFile("testdata/bulk_sitemap/Dockerfile")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "Dockerfile"), dockerfile, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "data.txt"), []byte(strings.Join(lines, "\n")+"\n"), 0644))
	return buildBulkTestImage(t, contextDir)
}

func TestBulkSitemapReplacesPreviousHarvestAndSkipsDuplicates(t *testing.T) {
	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/bulk": {
				StatusCode: 200,
				File:       "testdata/bulk_sitemap/sitemap.xml",
			},
		})

	localStorage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	harvest := func(imageName string) pkg.SitemapCrawlStats {
		sitemap, err := NewSitemap(context.Background(), mockedClient, 1, localStorage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
		require.NoError(t, err)
		sitemap.URL[0].Loc = imageName
		config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
		require.NoError(t, err)
		stats, err := sitemap.Harvest(context.Background(), &config)
		require.NoError(t, err)
		return stats
	}

	harvest(buildBulkTestImage(t, "./testdata/bulk_sitemap"))
	require.Len(t, readBulkFeatures(t, localStorage, "test_sitemap"), 3)

	stats := harvest(buildBulkTestImageWithData(t, []string{
		`{"@id": "https://example.com/a", "https://schema.org/name": "first"}`,
		`{"@id": "https://example.com/b"}`,
		`{"@id": "https://example.com/a", "https://schema.org/name": "duplicate"}`,
	}))
	require.Equal(t, 3, stats.SitesInSitemap)
	require.Equal(t, 2, stats.SuccessfulSites, "documents with a duplicate @id should not be counted twice")

	features := readBulkFeatures(t, localStorage, "test_sitemap")
	require.Len(t, features, 2, "documents from the previous harvest that are no longer in the container output should be removed")
	// documents are processed concurrently so which duplicate is kept is not deterministic
	require.Contains(t, []string{"first", "duplicate"}, features["https://example.com/a"].Name)
	require.Contains(t, features, "https://example.com/b")
}

// a shacl client that is slow to respond and records how many requests it handled at once
type slowShaclValidatorClient struct {
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (m *slowShaclValidatorClient) Validate(ctx context.Context, in *protoBuild.JsoldValidationRequest, opts ...grpc.CallOption) (*protoBuild.ValidationReply, error) {
	current := m.inFlight.Add(1)
	defer m.inFlight.Add(-1)
	for {
		previousMax := m.maxInFlight.Load()
		if current <= previousMax || m.maxInFlight.CompareAndSwap(previousMax, current) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	return &protoBuild.ValidationReply{Valid: true, Message: "valid"}, nil
}

func TestBulkSitemapValidatesShaclConcurrently(t *testing.T) {
	const numDocs = 300

	contextDir := t.TempDir()
	dockerfile, err := os.ReadFile("testdata/bulk_sitemap/Dockerfile")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "Dockerfile"), dockerfile, 0644))
	var data strings.Builder
	for i := range numDocs {
		fmt.Fprintf(&data, `{"@context":{"schema":"https://schema.org/"},"@type":"schema:Place","@id":"https://example.com/items/%d"}`+"\n", i)
	}
	require.NoError(t, os.WriteFile(filepath.Join(contextDir, "data.txt"), []byte(data.String()), 0644))

	imageName := buildBulkTestImage(t, contextDir)

	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/bulk": {
				StatusCode: 200,
				File:       "testdata/bulk_sitemap/sitemap.xml",
			},
		})

	localStorage, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, localStorage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)
	sitemap.URL[0].Loc = imageName

	shaclClient := &slowShaclValidatorClient{}
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, NewGrpcShaclValidatorFromClients(shaclClient), false)
	require.NoError(t, err)

	start := time.Now()
	stats, err := sitemap.Harvest(context.Background(), &config)
	require.NoError(t, err)
	elapsed := time.Since(start)

	require.Equal(t, numDocs, stats.SuccessfulSites)
	require.Zero(t, stats.WarningStats.TotalShaclFailures)
	require.Greater(t, shaclClient.maxInFlight.Load(), int32(1), "shacl validation should run concurrently")
	serialTime := numDocs * 20 * time.Millisecond
	require.Less(t, elapsed, serialTime/2, "validating concurrently should be much faster than validating serially")

	require.Len(t, readBulkFeatures(t, localStorage, "test_sitemap"), numDocs)
}

func TestBulkSitemapLogsStderrOfFailedContainer(t *testing.T) {
	imageName := buildBulkTestImage(t, "./testdata/bulk_sitemap_failing")

	mockedClient := common.NewMockedClient(
		true,
		map[string]common.MockResponse{
			"https://geoconnex.us/sitemap/iow/bulk": {
				StatusCode: 200,
				File:       "testdata/bulk_sitemap/sitemap.xml",
			},
		})

	store, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, store, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)
	sitemap.URL[0].Loc = imageName

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, nil, false)
	require.NoError(t, err)

	hook := logrustest.NewGlobal()
	defer hook.Reset()

	_, err = sitemap.Harvest(context.Background(), &config)
	require.ErrorContains(t, err, "container exited with status 1")

	foundStderr := false
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "Traceback: failed to download the source data") {
			foundStderr = true
		}
	}
	require.True(t, foundStderr, "the stderr of the failed container should be logged")
}
