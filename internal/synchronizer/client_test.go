// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer

import (
	"context"
	"io"
	"log"
	"nabu/internal/common"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	testhelpers "nabu/testHelpers"
	"net/http"
	"os"
	"runtime/trace"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

func countSourcesInSitemap(url string) (int, error) {
	// Fetch the URL
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

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

type SynchronizerClientSuite struct {
	// struct that stores metadata about the test suite itself
	suite.Suite
	// the top level client for syncing between graphdb and minio
	client SynchronizerClient
	// minio container that gleaner will send data to
	minioContainer s3.MinioContainer
	// graphdb container that nabu will sync with
	graphdbContainer triplestore.GraphDBContainer
	// the docker network over which gleaner sends data to minio
	network *testcontainers.DockerNetwork
}

func (suite *SynchronizerClientSuite) SetupSuite() {

	ctx := context.Background()
	net, err := network.New(ctx)
	suite.Require().NoError(err)
	suite.network = net
	t := suite.T()
	config := s3.MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
		ContainerName: "synchronizerTestMinio",
		Network:       net.Name,
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

	graphdbContainer, err := triplestore.NewGraphDBContainer("iow", "./triplestore/testdata/iow-config.ttl")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer

	client, err := NewSynchronizerClientFromClients(&graphdbContainer.Client, suite.minioContainer.ClientWrapper, suite.minioContainer.ClientWrapper.DefaultBucket)
	require.NoError(t, err)
	suite.client = client
}

func (s *SynchronizerClientSuite) TearDownSuite() {
	err := testcontainers.TerminateContainer(*s.minioContainer.Container)
	require.NoError(s.T(), err)
	err = testcontainers.TerminateContainer(*s.graphdbContainer.Container)
	require.NoError(s.T(), err)

	f, err := os.Open("trace.out")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	trace.Stop()
}

func (suite *SynchronizerClientSuite) TestMoveObjToTriplestore() {
	t := suite.T()
	gleanerContainer, err := testhelpers.NewGleanerContainer("../../config/iow/gleanerconfig.yaml", []string{
		"--source", "cdss0",
		"--address", "synchronizerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	require.Zero(t, gleanerContainer.ExitCode, gleanerContainer.Logs)
	orgsObjs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{"orgs/"})
	require.NoError(t, err)
	require.Equal(t, orgsObjs, 1)
	sourcesInSitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml")
	require.NoError(t, err)
	summonedObjs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{"summoned/cdss0/"})
	require.NoError(t, err)
	require.Equal(t, sourcesInSitemap, summonedObjs)
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "orgs/", false)
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	require.Len(t, graphs, 1)

	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/", false)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/cdss0/")
	require.NoError(t, err)
	require.Len(t, graphs, sourcesInSitemap)

}

func (suite *SynchronizerClientSuite) TestMoveNqToTriplestore() {
	t := suite.T()
	gleanerContainer, err := testhelpers.NewGleanerContainer("../../config/iow/gleanerconfig.yaml", []string{
		"--source", "cdss0",
		"--address", "synchronizerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	require.Zero(t, gleanerContainer.ExitCode, gleanerContainer.Logs)
	err = suite.client.UploadNqFileToTriplestore("orgs/cdss0.nq")
	require.NoError(t, err)
}

func (suite *SynchronizerClientSuite) TestSyncTriplestore() {
	t := suite.T()
	err := suite.graphdbContainer.Client.ClearAllGraphs()
	require.NoError(suite.T(), err)
	// this is the urn version of orgs/
	// we insert this to make sure that it gets removed
	oldGraph := "urn:iow:orgs:dummy"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .`
	err = suite.graphdbContainer.Client.UpsertNamedGraphs([]common.NamedGraph{
		{
			GraphURI: oldGraph,
			Triples:  data,
		},
	})
	require.NoError(t, err)
	exists, err := suite.graphdbContainer.Client.GraphExists(oldGraph)
	require.NoError(t, err)
	require.True(t, exists)

	gleanerContainer, err := testhelpers.NewGleanerContainer("../../config/iow/gleanerconfig.yaml", []string{
		"--source", "cdss0",
		"--address", "synchronizerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	require.Zero(t, gleanerContainer.ExitCode, gleanerContainer.Logs)
	sourcesInCdss0Sitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml")
	require.NoError(t, err)

	// make sure that an old graph is no longer there when
	// we sync new org data
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "orgs/", true)
	require.NoError(t, err)
	exists, err = suite.graphdbContainer.Client.GraphExists(oldGraph)
	require.False(t, exists)
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	// 1 graph should be associated with the orgs prefix; old one should be dropped
	require.Equal(t, len(graphs), 1)

	// make sure that there is prov data for every source in the sitemap
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "prov/", true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "prov/")
	require.NoError(t, err)
	require.Equal(t, len(graphs), sourcesInCdss0Sitemap)

	// make sure that after a prov sync that the org graph is still there
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "orgs/")
	require.NoError(t, err)
	require.Equal(t, len(graphs), 1)

	// make sure that summoned data matches the amount of sources in the sitemap
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/", true)
	require.NoError(t, err)
	graphs, err = suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix(context.Background(), "summoned/")
	require.NoError(t, err)
	require.Equal(t, len(graphs), sourcesInCdss0Sitemap)

	// Harvest another source to make sure that the sync works with a new source
	// syncing from the same prefix with more data this time
	gleanerContainer, err = testhelpers.NewGleanerContainer("../../config/iow/gleanerconfig.yaml", []string{
		"--source", "refgages0",
		"--address", "synchronizerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	require.Zero(t, gleanerContainer.ExitCode, gleanerContainer.Logs)
	sourcesInRefGagesSitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/ref/gages/gages__0.xml")
	require.NoError(t, err)

	// make sure that graph syncs are additive between sources and that
	// sources are not overwritten or removed
	err = suite.client.SyncTriplestoreGraphs(context.Background(), "summoned/refgages0", true)
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
	err := suite.graphdbContainer.Client.ClearAllGraphs()
	require.NoError(suite.T(), err)

	gleanerContainer, err := testhelpers.NewGleanerContainer("../../config/iow/gleanerconfig.yaml", []string{
		"--source", "cdss0",
		"--address", "synchronizerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	require.Zero(t, gleanerContainer.ExitCode, gleanerContainer.Logs)

	err = suite.client.GenerateNqRelease("orgs/cdss0")
	require.NoError(t, err)
	const orgsPath = "graphs/latest/cdss0_organizations.nq"
	objs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{orgsPath})
	require.NoError(t, err)
	require.Equal(t, objs, 1)

	err = suite.client.GenerateNqRelease("summoned/cdss0")
	require.NoError(t, err)
	const summonedPath = "graphs/latest/cdss0_release.nq"
	objs, err = suite.client.S3Client.NumberOfMatchingObjects([]string{summonedPath})
	require.NoError(t, err)
	require.Equal(t, objs, 1)

	summonedContent, err := suite.client.S3Client.GetObjectAsBytes(summonedPath)
	require.NoError(t, err)
	require.Contains(t, string(summonedContent), "<https://schema.org/subjectOf>")

	err = suite.client.UploadNqFileToTriplestore(orgsPath)
	require.NoError(t, err)
}

func (suite *SynchronizerClientSuite) TestGraphDiff() {
	t := suite.T()
	err := suite.graphdbContainer.Client.ClearAllGraphs()
	require.NoError(suite.T(), err)
	oldGraph := "urn:iow:testgraph:dummy"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .`
	err = suite.graphdbContainer.Client.UpsertNamedGraphs([]common.NamedGraph{{GraphURI: oldGraph, Triples: data}})
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
	require.Equal(t, diff.TriplestoreGraphsNotInS3, []string{"urn:iow:testgraph:dummy"})
	require.Equal(t, diff.s3UrnToAssociatedObjName, map[string]string{
		"urn:iow:testgraph:hu02.jsonld": "testgraph/hu02.jsonld",
		"urn:iow:testgraph:test.nq":     "testgraph/test.nq",
	})
}

func TestSynchronizerClientSuite(t *testing.T) {
	f, err := os.Create("trace.out")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if err := trace.Start(f); err != nil {
		log.Fatal(err)
	}

	suite.Run(t, new(SynchronizerClientSuite))
}
