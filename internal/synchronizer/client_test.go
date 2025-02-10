package synchronizer

import (
	"context"
	"io"
	"nabu/internal/synchronizer/objects"
	"nabu/internal/synchronizer/triplestore"
	testhelpers "nabu/testHelpers"
	"net/http"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

type SynchronizerClientSuite struct {
	suite.Suite

	// the top level client for syncing between graphdb and minio
	client SynchronizerClient
	// minio container that gleaner will send data to
	minioContainer objects.MinioContainer
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

	config := objects.MinioContainerConfig{
		Username:      "amazingaccesskey",
		Password:      "amazingsecretkey",
		DefaultBucket: "iow",
		ContainerName: "synchronizerTestMinio",
		Network:       net.Name,
	}

	minioContainer, err := objects.NewMinioContainer(config)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(ctx, suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

	graphdbContainer, err := triplestore.NewGraphDBContainer("iow", "./triplestore/testdata/iow-config.ttl")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer

	client, err := NewSynchronizerClient(&graphdbContainer.Client, suite.minioContainer.ClientWrapper, suite.minioContainer.ClientWrapper.DefaultBucket)
	require.NoError(suite.T(), err)
	suite.client = client
}

func (s *SynchronizerClientSuite) TearDownSuite() {
	err := testcontainers.TerminateContainer(*s.minioContainer.Container)
	require.NoError(s.T(), err)
	err = testcontainers.TerminateContainer(*s.graphdbContainer.Container)
	require.NoError(s.T(), err)
}

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
	err = suite.client.CopyAllPrefixedObjToTriplestore([]string{"orgs"})
	require.NoError(t, err)
	graphs, err := suite.client.GraphClient.NamedGraphsAssociatedWithS3Prefix("orgs")
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

func TestSynchronizerClientSuite(t *testing.T) {
	suite.Run(t, new(SynchronizerClientSuite))
}
