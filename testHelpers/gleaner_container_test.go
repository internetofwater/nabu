package testhelpers

import (
	"context"
	"io"
	"nabu/internal/synchronizer/objects"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type GleanerContainerSuite struct {
	suite.Suite
	// minio container that we use for storing gleaner output data
	minioContainer objects.MinioContainer
}

func (suite *GleanerContainerSuite) SetupSuite() {
	minioConfig := objects.MinioContainerConfig{
		ContainerName: "gleaner_test_minio",
		Username:      "amazingaccesskey",
		Password:      "amazingsecretkey",
		DefaultBucket: "gleanerbucket",
	}
	minioContainer, err := objects.NewMinioContainer(minioConfig)
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer
}

func (suite *GleanerContainerSuite) TestGleanerContainerHelpMsg() {
	t := suite.T()
	gleaner, err := NewGleanerContainer("../config/iow/gleanerconfig.yaml", []string{"--help"})
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
	gleaner, err := NewGleanerContainer("../config/iow/gleanerconfig.yaml", []string{"--source", "cdss0", "--address", "nabu_test_minio"})
	require.NoError(t, err)
	logs, err := gleaner.Container.Logs(context.Background())
	require.NoError(t, err)
	_, err = io.ReadAll(logs)
	defer logs.Close() // Close after reading
	require.NoError(t, err)

}

func TestGleanerContainerSuite(t *testing.T) {
	suite.Run(t, new(GleanerContainerSuite))
}
