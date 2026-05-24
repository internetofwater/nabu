// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

// Run harvest and output the data to minio so it can be synchronized
func NewHarvestRun(client *http.Client, minioClient *s3.MinioClientWrapper, sitemap, source string) error {
	index, err := crawl.NewSitemapIndex(sitemap, client)
	if err != nil {
		return err
	}
	_, err = index.
		WithStorageDestination(minioClient).
		WithConcurrencyConfig(10, 10).
		WithSpecifiedSourceFilter(source).
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

func (suite *SynchronizerClientSuite) TestNqRelease() {
	t := suite.T()
	const source = "cdss:co_gages__0"
	const graphAndItsAssociatedHash = 2

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

	err := NewHarvestRun(mockedClient, suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)

	t.Run("error is thrown with bad mainstem info", func(t *testing.T) {
		err = suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "orgs/" + source, AddMainstems: true}, false, "THIS_FILE_DOESNT_EXIST.txt")
		require.Error(t, err)
		require.ErrorContains(t, err, "does not exist locally")
	})

	t.Run("error is thrown if requring mainstem info but no mainstem file provided", func(t *testing.T) {
		err = suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "orgs/" + source, AddMainstems: true}, false, "")
		require.Error(t, err)
		require.ErrorContains(t, err, "but no mainstem file was provided")
	})

	t.Run("mainstem info can be added", func(t *testing.T) {
		err = suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "summoned/" + source, AddMainstems: true}, false, "testdata/colorado_subset.fgb")
		require.NoError(t, err)
		const releaseGraphPath = "graphs/latest/" + source + "_release.nq"
		data, err := suite.client.S3Client.GetObjectAsBytes(releaseGraphPath)
		require.NoError(t, err)
		// HAYDITCO should be associated with mainstem 36825
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/f316dbc9cd7aa3daf71f52f5a8e1b9679f0fd10c8f30f3a7891fc7c597f955a8> <https://schema.org/value> \"HAYDITCO\" <urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/8222bca6cb5c8b8714239f92b1607ae787b21b383a6aab2654069610667fee21> <https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/ref/mainstems/36825> <urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")
	})

	t.Run("mainstem info can be added even if one geometry is invalid", func(t *testing.T) {

		invalidDataPath := "summoned/" + source + "/invalid_data.jsonld"
		const selfIntersectingWkt = `{
			"@context": {
				"gsp": "http://www.opengis.net/ont/geosparql#",
				"sf": "http://www.opengis.net/ont/sf#"
			},
			"gsp:hasGeometry": {
				"@type": "http://www.opengis.net/ont/sf#Polygon",
				"gsp:asWKT": {
				"@type": "http://www.opengis.net/ont/geosparql#wktLiteral",
				"@value": "POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))"
				}
			}
		}`
		err := suite.client.S3Client.StoreWithoutServersideHash(invalidDataPath, strings.NewReader(selfIntersectingWkt))
		require.NoError(t, err)

		err = suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "summoned/" + source, AddMainstems: true}, false, "testdata/colorado_subset.fgb")
		require.NoError(t, err)
		const releaseGraphPath = "graphs/latest/" + source + "_release.nq"
		data, err := suite.client.S3Client.GetObjectAsBytes(releaseGraphPath)
		require.NoError(t, err)
		// HAYDITCO should be associated with mainstem 36825
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/f316dbc9cd7aa3daf71f52f5a8e1b9679f0fd10c8f30f3a7891fc7c597f955a8> <https://schema.org/value> \"HAYDITCO\" <urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/8222bca6cb5c8b8714239f92b1607ae787b21b383a6aab2654069610667fee21> <https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/ref/mainstems/36825> <urn:iow:summoned:cdss:co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")
		require.Contains(t, string(data), "POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))", "The invalid WKT should still be present, but it just doesn't link to a mainstem")
	})

	t.Run("generate nq release for summoned", func(t *testing.T) {
		err := suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "summoned/" + source, AddMainstems: false}, false, "")
		require.NoError(t, err)
		const summonedPath = "graphs/latest/" + source + "_release.nq"
		objs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{summonedPath})
		require.NoError(t, err)
		require.Equal(t, graphAndItsAssociatedHash, objs)

		summonedContent, err := suite.client.S3Client.GetObjectAsBytes(summonedPath)
		require.NoError(t, err)
		require.Contains(t, string(summonedContent), "<https://schema.org/subjectOf>")

		hashOfUncompressedData, err := suite.client.S3Client.GetObjectAsBytes(summonedPath + ".bytesum")
		require.NoError(t, err, "associated hash should exist")

		t.Run("compressed version of release graph", func(t *testing.T) {
			err = suite.client.GenerateNqRelease(context.Background(), crawl.SitemapMetadata{SitemapID: "summoned/" + source, AddMainstems: false}, true, "")
			require.NoError(t, err)
			const compressedReleaseGraph = "graphs/latest/" + source + "_release.nq.gz"
			zippedContent, err := suite.client.S3Client.GetObjectAsBytes(compressedReleaseGraph)
			require.NoError(t, err)
			require.NotContains(t, string(zippedContent), "<https://schema.org/subjectOf>", "graph should be compressed, but was raw n-quads")

			unzipper, err := gzip.NewReader(bytes.NewReader(zippedContent))
			require.NoError(t, err)
			unzippedContent, err := io.ReadAll(unzipper)
			require.NoError(t, err)
			err = unzipper.Close()
			require.NoError(t, err)
			require.Contains(t, string(unzippedContent), "<https://schema.org/subjectOf>", "when unzipped, the graph should have the same content")

			quads := strings.Split(string(unzippedContent), "\n")

			subsetOfGraph := quads[:500]
			summonedContentAsString := string(summonedContent)
			// we loop over only a subset of the graph since the graph is very large and has thousands of quads, otherwise slowing down our tests
			for _, quad := range subsetOfGraph {
				require.Contains(t, summonedContentAsString, quad, fmt.Sprintf("quad %s should be in the original graph", quad))
			}

			t.Run("hash is correct", func(t *testing.T) {
				hashOfCompressedData, err := suite.client.S3Client.GetObjectAsBytes(compressedReleaseGraph + ".bytesum")
				require.NoError(t, err)
				require.NotEqual(t, string(hashOfUncompressedData), string(hashOfCompressedData), "the hash of the compressed graph should be different from the uncompressed one")

				unzippedHash := fmt.Sprintf("%d", common.ByteSum(unzippedContent))
				require.Equal(t, string(hashOfUncompressedData), unzippedHash)

			})
		})

	})
}

func TestSynchronizerClientSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerClientSuite))
}
