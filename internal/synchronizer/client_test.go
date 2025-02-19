package synchronizer

import (
	"context"
	"io"
	"log"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	testhelpers "nabu/testHelpers"
	"net/http"
	"os"
	"runtime/trace"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
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

	minioContainer, err := s3.NewMinioContainer(config)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	stopHealthCheck, err := suite.minioContainer.ClientWrapper.Client.HealthCheck(5 * time.Second)
	require.NoError(t, err)
	defer stopHealthCheck()
	require.True(t, suite.minioContainer.ClientWrapper.Client.IsOnline())

	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(ctx, suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(t, err)

	graphdbContainer, err := triplestore.NewGraphDBContainer("iow", "./triplestore/testdata/iow-config.ttl")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer

	client, err := newSynchronizerClient(&graphdbContainer.Client, suite.minioContainer.ClientWrapper, suite.minioContainer.ClientWrapper.DefaultBucket)
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
	err = suite.client.CopyAllPrefixedObjToTriplestore([]string{"orgs/"})
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix("orgs/")
	require.NoError(t, err)
	require.Len(t, graphs, 1)
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
	oldGraph := "urn:iow:orgs"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .`
	err = suite.graphdbContainer.Client.InsertWithNamedGraph(data, oldGraph)
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
	sourcesInSitemap, err := countSourcesInSitemap("https://pids.geoconnex.dev/sitemap/cdss/co_gages__0.xml")
	require.NoError(t, err)

	t.Run("sync org graph", func(t *testing.T) {
		err = suite.client.SyncTriplestoreGraphs([]string{"orgs/"})
		require.NoError(t, err)
		// make sure that an old graph is no longer there after sync
		exists, err = suite.graphdbContainer.Client.GraphExists(oldGraph)
		require.False(t, exists)
		require.NoError(t, err)
		graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix("orgs/")
		require.NoError(t, err)
		require.Equal(t, len(graphs), 1)
	})

	t.Run("sync prov graphs", func(t *testing.T) {
		err = suite.client.SyncTriplestoreGraphs([]string{"prov/"})
		require.NoError(t, err)
		graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix("prov/")
		require.NoError(t, err)
		require.Equal(t, len(graphs), sourcesInSitemap)
	})

	t.Run("sync summoned graphs", func(t *testing.T) {
		err = suite.client.SyncTriplestoreGraphs([]string{"summoned/"})
		require.NoError(t, err)
		graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix("summoned/")
		require.NoError(t, err)
		require.Equal(t, len(graphs), sourcesInSitemap)
	})
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

	err = suite.client.GenerateNqRelease([]string{"orgs/cdss0"})
	require.NoError(t, err)
	const nqPath = "graphs/latest/cdss0_organizations.nq"
	objs, err := suite.client.S3Client.NumberOfMatchingObjects([]string{nqPath})
	require.NoError(t, err)
	require.Equal(t, objs, 1)

	err = suite.client.UploadNqFileToTriplestore(nqPath)
	require.NoError(t, err)
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
