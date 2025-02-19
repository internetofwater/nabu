package cli

import (
	"context"
	"fmt"
	"nabu/internal/common/projectpath"
	"nabu/internal/synchronizer/s3"
	"os"
	"path/filepath"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// Wrapper struct to store a handle to the container for all
type RootCliSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

func (suite *RootCliSuite) SetupSuite() {
	config := s3.MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
	}
	minioContainer, err := s3.NewMinioContainer(config)
	require.NoError(suite.T(), err)
	suite.minioContainer = minioContainer

	// create the bucket
	err = suite.minioContainer.ClientWrapper.Client.MakeBucket(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, minio.MakeBucketOptions{})
	require.NoError(suite.T(), err)

}

func (s *RootCliSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

func (suite *RootCliSuite) TestRootCmdWithTracing() {
	t := suite.T()

	os.Setenv("NABU_PROFILING", "True")
	args := []string{"test", "--cfg", filepath.Join(projectpath.Root, "config", "iow", "nabuconfig.yaml"), "--address", suite.minioContainer.Hostname, "--port", fmt.Sprint(suite.minioContainer.APIPort), suite.minioContainer.Hostname}
	rootCmd.SetArgs(args)
	Execute()
	_, err := os.Stat(filepath.Join(projectpath.Root, "trace.out"))
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(projectpath.Root, "http_trace.csv"))
	require.NoError(t, err)

	_, err = suite.minioContainer.ClientWrapper.Client.StatObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, "http_trace.csv", minio.StatObjectOptions{})
	require.NoError(t, err)

}

// Run the entire test suite
func TestRootClientSuite(t *testing.T) {
	suite.Run(t, new(RootCliSuite))
}
