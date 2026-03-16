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
	index, err := crawl.NewSitemapIndexHarvester(sitemap, client)
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
	const source = "cdss_co_gages__0"
	const graphAndItsAssociatedHash = 2
	err := NewHarvestRun(common.NewCrawlerClient(), suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)

	t.Run("error is thrown with bad mainstem info", func(t *testing.T) {
		err = suite.client.GenerateNqRelease(context.Background(), "orgs/"+source, false, "THIS_FILE_DOESNT_EXIST.txt")
		require.Error(t, err)
		require.ErrorContains(t, err, "does not exist locally")
	})

	t.Run("mainstem info can be added", func(t *testing.T) {
		err = suite.client.GenerateNqRelease(context.Background(), "summoned/"+source, false, "testdata/colorado_subset.fgb")
		require.NoError(t, err)
		const releaseGraphPath = "graphs/latest/" + source + "_release.nq"
		data, err := suite.client.S3Client.GetObjectAsBytes(releaseGraphPath)
		require.NoError(t, err)
		// HAYDITCO should be associated with mainstem 36825
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/f316dbc9cd7aa3daf71f52f5a8e1b9679f0fd10c8f30f3a7891fc7c597f955a8> <https://schema.org/value> \"HAYDITCO\" <urn:iow:summoned:cdss_co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")
		require.Contains(t, string(data), "<https://docs.geoconnex.us/nqhash/8222bca6cb5c8b8714239f92b1607ae787b21b383a6aab2654069610667fee21> <https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://geoconnex.us/ref/mainstems/36825> <urn:iow:summoned:cdss_co_gages__0:aHR0cHM6Ly9waWRzLmdlb2Nvbm5leC5kZXYvY2Rzcy9nYWdlcy9IQVlESVRDTw==.jsonld> .")

	})

	t.Run("generate nq release for orgs", func(t *testing.T) {

		err = suite.client.GenerateNqRelease(context.Background(), "orgs/"+source, false, "")
		require.NoError(t, err)
		const orgsPath = "graphs/latest/" + source + "_organizations.nq"
		objs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{orgsPath})
		require.NoError(t, err)
		require.Equal(t, graphAndItsAssociatedHash, objs)

		t.Run("hash is correct", func(t *testing.T) {
			bytes, err := suite.client.S3Client.GetObjectAsBytes(orgsPath)
			manuallyCalculatedHash := common.ByteSum(bytes)
			require.NoError(t, err)
			hashInMinio, err := suite.client.S3Client.GetObjectAsBytes(orgsPath + ".bytesum")
			require.NoError(t, err)
			require.Equal(t, string(hashInMinio), fmt.Sprintf("%d", manuallyCalculatedHash))
		})
	})

	t.Run("generate nq release for summoned", func(t *testing.T) {
		err := suite.client.GenerateNqRelease(context.Background(), "summoned/"+source, false, "")
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
			err = suite.client.GenerateNqRelease(context.Background(), "summoned/"+source, true, "")
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
