// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/internal/synchronizer/triplestores"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

func countSourcesInSitemap(url string) (int, error) {
	// Fetch the URL
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	// /loc represents the closing tag of a <loc> item in an xml sitemap
	// thus the number of /loc tags is the number of sources
	count := strings.Count(string(body), "/loc")
	return count, nil
}

// Run gleaner and output the data to minio so it can be synchronized
func NewGleanerRun(minioClient *s3.MinioClientWrapper, sitemap, source string) error {
	index, err := crawl.NewSitemapIndexHarvester(sitemap)
	if err != nil {
		return err
	}
	_, err = index.
		WithStorageDestination(minioClient).
		WithConcurrencyConfig(10, 10).
		WithSpecifiedSourceFilter(source).
		HarvestSitemaps(context.Background())
	return err
}

type SynchronizerClientSuite struct {
	// struct that stores metadata about the test suite itself
	suite.Suite
	// the top level client for syncing between graphdb and minio
	client SynchronizerClient
	// minio container that gleaner will send data to
	minioContainer s3.MinioContainer
	// graphdb container that nabu will sync with
	graphdbContainer triplestores.GraphDBContainer
}

func (suite *SynchronizerClientSuite) SetupSuite() {

	t := suite.T()
	config := s3.MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
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

	err = suite.minioContainer.ClientWrapper.MakeDefaultBucket()
	require.NoError(t, err)

	graphdbContainer, err := triplestores.NewGraphDBContainer("iow", filepath.Join("triplestores", "testdata", "iow-config.ttl"))
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer

	client, err := NewSynchronizerClientFromClients(&graphdbContainer.Client, suite.minioContainer.ClientWrapper, suite.minioContainer.ClientWrapper.DefaultBucket)
	require.NoError(t, err)
	suite.client = client
}

func (s *SynchronizerClientSuite) TearDownSuite() {
	err := testcontainers.TerminateContainer(*s.minioContainer.Container)
	s.Require().NoError(err)
	err = testcontainers.TerminateContainer(*s.graphdbContainer.Container)
	s.Require().NoError(err)
}

func (suite *SynchronizerClientSuite) TestMoveObjToTriplestore() {
	t := suite.T()

	const source = "cdss_co_gages__0"
	err := NewGleanerRun(suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)

	orgsObjs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{"orgs/"})
	require.NoError(t, err)
	require.Equal(t, 1, orgsObjs)
	sourcesInSitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml")
	require.NoError(t, err)
	summonedObjs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{"summoned/" + source + "/"})
	require.NoError(t, err)
	require.Equal(t, sourcesInSitemap, summonedObjs)
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "orgs/", false)
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	require.Len(t, graphs, 1)
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/", false)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/"+source+"/")
	require.NoError(t, err)
	require.Len(t, graphs, sourcesInSitemap)

}

func (suite *SynchronizerClientSuite) TestMoveNqToTriplestore() {
	t := suite.T()
	const source = "cdss_co_gages__0"
	err := NewGleanerRun(suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)
	err = suite.client.UploadNqFileToTriplestore("orgs/" + source + ".nq")
	require.NoError(t, err)
}

func (suite *SynchronizerClientSuite) TestSyncTriplestore() {
	t := suite.T()
	err := suite.graphdbContainer.Client.ClearAllGraphs()
	suite.Require().NoError(err)
	// this is the urn version of orgs/
	// we insert this to make sure that it gets removed
	oldGraph := "urn:iow:orgs:dummy"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .`
	err = suite.graphdbContainer.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{
		{
			GraphURI: oldGraph,
			Triples:  data,
		},
	})
	require.NoError(t, err)
	exists, err := suite.graphdbContainer.Client.GraphExists(context.Background(), oldGraph)
	require.NoError(t, err)
	require.True(t, exists)

	const source = "cdss_co_gages__0"
	err = NewGleanerRun(suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)
	sourcesInCdss0Sitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml")
	require.NoError(t, err)

	// make sure that an old graph is no longer there when
	// we sync new org data
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "orgs/", true)
	require.NoError(t, err)
	exists, err = suite.graphdbContainer.Client.GraphExists(context.Background(), oldGraph)
	require.False(t, exists)
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	// 1 graph should be associated with the orgs prefix; old one should be dropped
	require.Equal(t, 1, len(graphs))

	// make sure that there is prov data for every source in the sitemap
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "prov/", true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "prov/")
	require.NoError(t, err)
	require.Equal(t, 1, len(graphs))

	// make sure that after a prov sync that the org graph is still there
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	require.Equal(t, 1, len(graphs))

	// make sure that summoned data matches the amount of sources in the sitemap
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/", true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/")
	require.NoError(t, err)
	require.Len(t, graphs, sourcesInCdss0Sitemap)

	// Harvest another source to make sure that the sync works with a new source
	// syncing from the same prefix with more data this time
	const gages = "ref_gages_gages__0"
	err = NewGleanerRun(suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", gages)
	require.NoError(t, err)
	sourcesInRefGagesSitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/ref/gages/gages__0.xml")
	require.NoError(t, err)

	// make sure that graph syncs are additive between sources and that
	// sources are not overwritten or removed
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/"+gages, true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/")
	require.NoError(t, err)
	require.Equal(t, len(graphs), sourcesInCdss0Sitemap+sourcesInRefGagesSitemap)

	// delete 1 item from the s3 bucket and make sure that after
	// we sync again, we have the same number of graphs - 1
	objs, err := suite.client.S3Client.ObjectList(context.Background(), "summoned/")
	require.NoError(t, err)
	err = suite.client.S3Client.Remove(objs[0].Key)
	require.NoError(t, err)
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/", true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/")
	require.NoError(t, err)
	require.Equal(t, len(graphs), sourcesInCdss0Sitemap+sourcesInRefGagesSitemap-1)

}

func (suite *SynchronizerClientSuite) TestNqRelease() {
	t := suite.T()

	const source = "cdss_co_gages__0"
	err := NewGleanerRun(suite.minioContainer.ClientWrapper, "https://pids.geoconnex.dev/sitemap.xml", source)
	require.NoError(t, err)

	err = suite.client.GenerateNqRelease("orgs/" + source)
	require.NoError(t, err)
	const orgsPath = "graphs/latest/" + source + "_organizations.nq"
	objs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{orgsPath})
	require.NoError(t, err)
	const graphAndItsAssociatedHash = 2
	require.Equal(t, graphAndItsAssociatedHash, objs)

	err = suite.client.GenerateNqRelease("summoned/" + source)
	require.NoError(t, err)
	const summonedPath = "graphs/latest/" + source + "_release.nq"
	objs, err = suite.client.S3Client.NumberOfMatchingObjects([]string{summonedPath})
	require.NoError(t, err)
	require.Equal(t, graphAndItsAssociatedHash, objs)

	summonedContent, err := suite.client.S3Client.GetObjectAsBytes(summonedPath)
	require.NoError(t, err)
	require.Contains(t, string(summonedContent), "<https://schema.org/subjectOf>")

	err = suite.client.UploadNqFileToTriplestore(orgsPath)
	require.NoError(t, err)
}

func (suite *SynchronizerClientSuite) TestGraphDiff() {
	t := suite.T()
	err := suite.graphdbContainer.Client.ClearAllGraphs()
	suite.Require().NoError(err)
	oldGraph := "urn:iow:testgraph:dummy"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .`
	err = suite.graphdbContainer.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{{GraphURI: oldGraph, Triples: data}})
	require.NoError(t, err)

	err = suite.client.S3Client.UploadFile("testgraph/hu02.jsonld", "testdata/hu02.jsonld")
	require.NoError(t, err)
	defer func() {
		err = suite.client.S3Client.Remove("testdata/hu02.jsonld")
		require.NoError(t, err)
	}()

	err = suite.client.S3Client.UploadFile("testgraph/test.nq", "testdata/test.nq")
	require.NoError(t, err)

	defer func() {
		err = suite.client.S3Client.Remove("testdata/test.nq")
		require.NoError(t, err)
	}()

	diff, err := suite.client.getGraphDiff(context.Background(), "testgraph/")
	require.NoError(t, err)
	require.Contains(t, diff.S3GraphsNotInTriplestore, "urn:iow:testgraph:hu02.jsonld")
	require.Contains(t, diff.S3GraphsNotInTriplestore, "urn:iow:testgraph:test.nq")
	require.Equal(t, []string{"urn:iow:testgraph:dummy"}, diff.TriplestoreGraphsNotInS3)
	require.Equal(t, map[string]string{
		"urn:iow:testgraph:hu02.jsonld": "testgraph/hu02.jsonld",
		"urn:iow:testgraph:test.nq":     "testgraph/test.nq",
	}, diff.s3UrnToAssociatedObjName)
}

func TestSynchronizerClientSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerClientSuite))
}
