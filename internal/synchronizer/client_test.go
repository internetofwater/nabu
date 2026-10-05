// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

// Run harvest and output the data to minio so it can be synchronized
func NewHarvestRun(client *http.Client, minioClient *s3.MinioClientWrapper, sitemap, source, mainstemFile string) error {
	index, err := crawl.NewSitemapIndex(sitemap, client)
	if err != nil {
		return err
	}
	_, err = index.
		WithStorageDestination(minioClient).
		WithConcurrencyConfig(10, 10).
		WithSpecifiedSourceFilter(source).
		WithMainstemFile(mainstemFile).
		HarvestSitemaps(context.Background(), client)
	return err
}

type SynchronizerClientSuite struct {
	// struct that stores metadata about the test suite itself
	suite.Suite
	// the top level client for syncing between graphdb and minio
	client SynchronizerClient
	// minio container that nabu harvest will send data to
	minioContainer s3.MinioContainer
}

func (suite *SynchronizerClientSuite) SetupSuite() {

	t := suite.T()
	config := s3.MinioContainerConfig{
		Username:       "minioadmin",
		Password:       "minioadmin",
		DefaultBucket:  "iow",
		MetadataBucket: "iow-metadata",
	}

	minioContainer, err := s3.NewMinioContainerFromConfig(config)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	stopHealthCheck, err := suite.minioContainer.ClientWrapper.Client.HealthCheck(5 * time.Second)
	require.NoError(t, err)
	defer stopHealthCheck()
	require.Eventually(t, func() bool {
		return suite.minioContainer.ClientWrapper.Client.IsOnline()
	}, 10*time.Second, 500*time.Millisecond, "MinIO container did not become online in time")

	err = suite.minioContainer.ClientWrapper.SetupBuckets()
	require.NoError(t, err)

	client, err := NewSynchronizerClientFromClients(suite.minioContainer.ClientWrapper, suite.minioContainer.ClientWrapper.DefaultBucket, suite.minioContainer.ClientWrapper.MetadataBucket)
	require.NoError(t, err)
	suite.client = client
}

func (s *SynchronizerClientSuite) TearDownSuite() {
	err := testcontainers.TerminateContainer(*s.minioContainer.Container)
	s.Require().NoError(err)
}

func (suite *SynchronizerClientSuite) TestNquads() {
	t := suite.T()
	const source = "cdss:co_gages__0"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://pids.geoconnex.dev/sitemap.xml":                  {File: "testdata/pids/sitemap_index.xml", StatusCode: 200},
		"https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml": {File: "testdata/pids/cdss_sitemap.xml", StatusCode: 200},

		"https://pids.geoconnex.dev/cdss/gages/BASMOUCO": {File: "testdata/pids/BASMOUCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/CHEREDCO": {File: "testdata/pids/CHEREDCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/ENTDITCO": {File: "testdata/pids/ENTDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/FARMERCO": {File: "testdata/pids/FARMERCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/FLOBONCO": {File: "testdata/pids/FLOBONCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/FLOCANCO": {File: "testdata/pids/FLOCANCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/FLOFARCO": {File: "testdata/pids/FLOFARCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/FREDITCO": {File: "testdata/pids/FREDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/GOVDRACO": {File: "testdata/pids/GOVDRACO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/HABREDCO": {File: "testdata/pids/HABREDCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/HAYDITCO": {File: "testdata/pids/HAYDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/HAYREDCO": {File: "testdata/pids/HAYREDCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LAPBRECO": {File: "testdata/pids/LAPBRECO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LAPCHECO": {File: "testdata/pids/LAPCHECO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LAPHESCO": {File: "testdata/pids/LAPHESCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LAPLONCO": {File: "testdata/pids/LAPLONCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LAPMEXCO": {File: "testdata/pids/LAPMEXCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LITCANCO": {File: "testdata/pids/LITCANCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LONALOCO": {File: "testdata/pids/LONALOCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LONBLOCO": {File: "testdata/pids/LONBLOCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LONREDCO": {File: "testdata/pids/LONREDCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/LPIRDICO": {File: "testdata/pids/LPIRDICO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/PINDITCO": {File: "testdata/pids/PINDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/PIODITCO": {File: "testdata/pids/PIODITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/REVDITCO": {File: "testdata/pids/REVDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/SALTOXCO": {File: "testdata/pids/SALTOXCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/TOWCANCO": {File: "testdata/pids/TOWCANCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/TOWEASCO": {File: "testdata/pids/TOWEASCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/TOWWESCO": {File: "testdata/pids/TOWWESCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/cdss/gages/VOSDITCO": {File: "testdata/pids/VOSDITCO.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://pids.geoconnex.dev/robots.txt":          {File: "testdata/pids/robots.txt", StatusCode: 200, ContentType: "text/plain"},
	})

	t.Run("harvest fails if a sitemap requests mainstems but no mainstem file is provided", func(t *testing.T) {
		err := NewHarvestRun(mockedClient, suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source, "")
		require.ErrorContains(t, err, "but no mainstem file was provided")
	})

	err := NewHarvestRun(mockedClient, suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source, "testdata/colorado_subset.fgb")
	require.NoError(t, err)

	const haydItcoGraph = "<urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld>"

	sources, err := suite.client.NewS3NquadsSources(context.Background(), "summoned/")
	require.NoError(t, err)
	require.Len(t, sources, 1)
	require.Equal(t, source, sources[0].SitemapID)

	t.Run("mainstems are put in the mainstem_uri column", func(t *testing.T) {
		reader, closer, err := sources[0].open(context.Background())
		require.NoError(t, err)
		defer func() { _ = closer.Close() }()
		mainstemsByID := map[string]string{}
		err = parquettable.Read(context.Background(), reader, func(f parquettable.Feature) error {
			mainstemsByID[f.ID] = f.MainstemURI
			return nil
		})
		require.NoError(t, err)
		require.Len(t, mainstemsByID, 30)
		// the source document for BASMOUCO does not reference a mainstem so it must come from the lookup
		require.Equal(t, "https://geoconnex.us/ref/mainstems/2597141", mainstemsByID["https://pids.geoconnex.dev/cdss/gages/BASMOUCO"])
		require.Equal(t, "https://geoconnex.us/ref/mainstems/42750", mainstemsByID["https://pids.geoconnex.dev/cdss/gages/FARMERCO"])
		require.Empty(t, mainstemsByID["https://pids.geoconnex.dev/cdss/gages/HAYDITCO"], "points outside of the mainstem file have no mainstem")
	})

	t.Run("mainstems added during harvest are in the nquads", func(t *testing.T) {
		var output bytes.Buffer
		err := suite.client.WriteNquads(context.Background(), &output, sources)
		require.NoError(t, err)
		const basmoucoGraph = "<urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9CQVNNT1VDTw==.jsonld>"
		require.Regexp(t, `<https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/ref/mainstems/2597141> `+regexp.QuoteMeta(basmoucoGraph)+` \.`, output.String())
	})

	t.Run("documents from a local file are named after the file", func(t *testing.T) {
		const doc = `{"@id": "https://example.com/local", "https://schema.org/name": "local"}`
		localParquet := filepath.Join(t.TempDir(), "local.parquet")
		file, err := os.Create(localParquet)
		require.NoError(t, err)
		writer, err := parquettable.NewWriter(file)
		require.NoError(t, err)
		feature, err := parquettable.FeatureFromJsonld([]byte(doc), "")
		require.NoError(t, err)
		require.NoError(t, writer.Write(feature))
		require.NoError(t, writer.Close())
		require.NoError(t, file.Close())

		var output bytes.Buffer
		err = suite.client.WriteNquads(context.Background(), &output, []NquadsSource{NewLocalNquadsSource(localParquet)})
		require.NoError(t, err)
		require.Contains(t, output.String(), "<urn:iow:summoned:local:", "documents from a local file should be in a graph named after the file")
	})

	t.Run("convert to nquads", func(t *testing.T) {
		var output bytes.Buffer
		err := suite.client.WriteNquads(context.Background(), &output, sources)
		require.NoError(t, err)
		require.Contains(t, output.String(), "<https://schema.org/subjectOf>")
		require.NotContains(t, output.String(), "_:", "blank nodes should be skolemized")

		graphs := map[string]struct{}{}
		for line := range strings.Lines(strings.TrimSpace(output.String())) {
			require.True(t, strings.HasSuffix(strings.TrimSpace(line), "> ."), "every line should be a quad: %s", line)
			fields := strings.Fields(line)
			graphs[fields[len(fields)-2]] = struct{}{}
		}
		require.Len(t, graphs, 30, "each of the 30 documents should be in its own graph")
		require.Contains(t, graphs, haydItcoGraph)
	})
}

func TestSynchronizerClientSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerClientSuite))
}
