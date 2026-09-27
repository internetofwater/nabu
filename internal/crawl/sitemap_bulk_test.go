// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/protoBuild"
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
	err = storage.StoreWithoutServersideHash("summoned/test_sitemap/stale.jsonld", bytes.NewReader([]byte("stale")))
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
	staleExists, err := storage.Exists("summoned/test_sitemap/stale.jsonld")
	require.NoError(t, err)
	require.False(t, staleExists)

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

// read the fixture the same way the bulk harvest reads container stdout
// and return the storage path and exact bytes of each document
func readBulkFixture(t *testing.T, sitemapID string) ([]string, [][]byte) {
	file, err := os.Open("testdata/bulk_sitemap/data.txt")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	paths := []string{}
	lines := [][]byte{}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var doc map[string]any
			require.NoError(t, json.Unmarshal(line, &doc))
			paths = append(paths, "summoned/"+sitemapID+"/"+base64.StdEncoding.EncodeToString([]byte(doc["@id"].(string)))+".jsonld")
			lines = append(lines, line)
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	return paths, lines
}

// wraps local storage and records which paths were uploaded in bulk
type countingBulkStorage struct {
	*storage.LocalTempFSCrawlStorage
	mu       sync.Mutex
	uploaded []string
}

func (c *countingBulkStorage) StoreBulk(ctx context.Context, items chan storage.BulkStorageItem) error {
	recorded := make(chan storage.BulkStorageItem)
	go func() {
		defer close(recorded)
		for item := range items {
			c.mu.Lock()
			c.uploaded = append(c.uploaded, item.Path)
			c.mu.Unlock()
			recorded <- item
		}
	}()
	return c.LocalTempFSCrawlStorage.StoreBulk(ctx, recorded)
}

func TestBulkSitemapOnlyUploadsChangedDocuments(t *testing.T) {
	imageName := buildBulkTestImage(t, "./testdata/bulk_sitemap")

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
	countingStorage := &countingBulkStorage{LocalTempFSCrawlStorage: localStorage}

	paths, lines := readBulkFixture(t, "test_sitemap")
	require.Len(t, paths, 3)

	// the first document is already stored and unchanged, the second is stored
	// with outdated content, the third is new, and one document is no longer in the container output
	require.NoError(t, localStorage.StoreWithoutServersideHash(paths[0], bytes.NewReader(lines[0])))
	require.NoError(t, localStorage.StoreWithoutServersideHash(paths[1], bytes.NewReader([]byte("outdated"))))
	require.NoError(t, localStorage.StoreWithoutServersideHash("summoned/test_sitemap/stale.jsonld", bytes.NewReader([]byte("stale"))))

	sitemap, err := NewSitemap(context.Background(), mockedClient, 1, countingStorage, SitemapMetadata{SitemapID: "test_sitemap", Loc: "https://geoconnex.us/sitemap/iow/bulk", BulkContainerImage: "test_bulk"})
	require.NoError(t, err)
	sitemap.URL[0].Loc = imageName

	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, &mockShaclValidatorClient{}, false, false)
	require.NoError(t, err)

	stats, _, err := sitemap.Harvest(context.Background(), &config)
	require.NoError(t, err)

	require.ElementsMatch(t, []string{paths[1], paths[2]}, countingStorage.uploaded, "only the changed and new documents should be uploaded")
	require.Equal(t, 3, stats.SuccessfulSites, "unchanged documents still count as successfully harvested")
	require.Equal(t, 3, stats.SitesInSitemap)

	for i, path := range paths {
		reader, err := localStorage.Get(path)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, string(lines[i]), string(data))
	}

	staleExists, err := localStorage.Exists("summoned/test_sitemap/stale.jsonld")
	require.NoError(t, err)
	require.False(t, staleExists, "documents no longer in the container output should be removed")
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
	config, err := NewSitemapHarvestConfig(mockedClient, sitemap, shaclClient, false, false)
	require.NoError(t, err)

	start := time.Now()
	stats, _, err := sitemap.Harvest(context.Background(), &config)
	require.NoError(t, err)
	elapsed := time.Since(start)

	require.Equal(t, numDocs, stats.SuccessfulSites)
	require.Zero(t, stats.WarningStats.TotalShaclFailures)
	require.Greater(t, shaclClient.maxInFlight.Load(), int32(1), "shacl validation should run concurrently")
	serialTime := numDocs * 20 * time.Millisecond
	require.Less(t, elapsed, serialTime/2, "validating concurrently should be much faster than validating serially")

	hashes, err := localStorage.ListHashes(context.Background(), "summoned/test_sitemap/")
	require.NoError(t, err)
	require.Len(t, hashes, numDocs)
}
