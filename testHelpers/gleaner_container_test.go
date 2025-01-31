package testhelpers

import (
	"context"
	"io"
	"nabu/internal/synchronizer/objects"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

type GleanerContainerSuite struct {
	suite.Suite
	// minio container that we use for storing gleaner output data
	minioContainer objects.MinioContainer
	network        *testcontainers.DockerNetwork
}

func (suite *GleanerContainerSuite) SetupSuite() {

	ctx := context.Background()
	net, err := network.New(ctx)
	suite.Require().NoError(err)
	suite.network = net

	minioConfig := objects.MinioContainerConfig{
		// note that the container name must be a full word with no special characters
		// this appears to mess with the docker network somehow and prevents connecting
		ContainerName: "gleanerTestMinio",
		Username:      "amazingaccesskey",
		Password:      "amazingsecretkey",
		DefaultBucket: "iow",
		Network:       net.Name,
	}
	minioContainer, err := objects.NewMinioContainer(minioConfig)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

}

func (suite *GleanerContainerSuite) TearDownSuite() {
	testcontainers.TerminateContainer(*suite.minioContainer.Container)
	testcontainers.CleanupNetwork(suite.T(), suite.network)
}

func (suite *GleanerContainerSuite) TestGleanerContainerHelpMsg() {
	t := suite.T()
	gleaner, err := NewGleanerContainer("../config/iow/gleanerconfig.yaml", []string{"--help"}, suite.network.Name)
	require.NoError(t, err)
	logs, err := gleaner.Container.Logs(context.Background())
	require.NoError(t, err)
	data, err := io.ReadAll(logs)
	defer logs.Close() // Close after reading
	require.NoError(t, err)
	require.Contains(t, string(data), "Extract JSON-LD from web pages exposed in a domains sitemap file")
}

func (suite *GleanerContainerSuite) TestGleanerHarvest() {
	t := suite.T()
	gleaner, err := NewGleanerContainer("../config/iow/gleanerconfig.yaml", []string{
		"--source", "cdss0",
		"--address", "gleanerTestMinio",
		"--setup",
		"--port", "9000",
	}, suite.network.Name)
	require.NoError(t, err)
	ctx := context.Background()
	name, err := gleaner.Container.Name(ctx)
	require.NoError(t, err)
	suite.T().Log(name)
	require.NoError(t, err)
	logs, err := gleaner.Container.Logs(context.Background())
	require.NoError(t, err)
	logBytes, err := io.ReadAll(logs)
	defer logs.Close() // Close after reading
	require.NoError(t, err)
	state, err := gleaner.Container.State(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, state.ExitCode, string(logBytes))

	orgsObjs, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	require.NoError(t, err)
	require.Equal(t, orgsObjs, 1)
	summonedObjs, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"summoned/cdss0/"})
	require.NoError(t, err)
	provObjs, err := suite.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"prov/cdss0/"})
	require.NoError(t, err)
	require.Equal(t, summonedObjs, provObjs)
}

func TestGleanerContainerSuite(t *testing.T) {
	suite.Run(t, new(GleanerContainerSuite))
}
