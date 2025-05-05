// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package gleaner_container

import (
	"context"
	"io"
	"nabu/internal/synchronizer/s3"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

type GleanerContainerSuite struct {
	suite.Suite
	// minio container that we use for storing gleaner output data
	minioContainer s3.MinioContainer
	network        *testcontainers.DockerNetwork
}

func (suite *GleanerContainerSuite) SetupSuite() {

	ctx := context.Background()
	net, err := network.New(ctx)
	suite.Require().NoError(err)
	suite.network = net

	minioConfig := s3.MinioContainerConfig{
		// note that the container name must be a full word with no special characters
		// this appears to mess with the docker network somehow and prevents connecting
		ContainerName: "gleanerContainerTestMinio",
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
		Network:       net.Name,
	}
	minioContainer, err := s3.NewMinioContainerFromConfig(minioConfig)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

}

func (suite *GleanerContainerSuite) TearDownSuite() {
	err := testcontainers.TerminateContainer(*suite.minioContainer.Container)
	require.NoError(suite.T(), err)
	testcontainers.CleanupNetwork(suite.T(), suite.network)
}

func (suite *GleanerContainerSuite) TestGleanerContainerHelpMsg() {
	t := suite.T()
	gleaner, err := NewGleanerContainer("--help", suite.network.Name)
	require.NoError(t, err)
	logs, err := gleaner.Container.Logs(context.Background())
	require.NoError(t, err)
	data, err := io.ReadAll(logs)
	defer logs.Close() // Close after reading
	require.NoError(t, err)
	require.Contains(t, string(data), "bucket")
	require.Contains(t, string(data), "address")
	require.Contains(t, string(data), "port")
}

func (suite *GleanerContainerSuite) TestGleanerHarvest() {
	t := suite.T()
	gleaner, err := NewGleanerContainer(
		"--sitemap-index https://pids.geoconnex.dev/sitemap.xml --source cdss_co_gages__0 --address gleanerContainerTestMinio",
		suite.network.Name)
	require.NoError(t, err)
	ctx := context.Background()
	name, err := gleaner.Container.Name(ctx)
	require.NoError(t, err)
	suite.T().Log(name)
	require.Equal(t, 0, gleaner.ExitCode, gleaner.Logs)

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
