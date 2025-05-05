// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package nabu

import (
	"context"
	"fmt"
	"nabu/internal/common/projectpath"
	"nabu/internal/synchronizer/s3"
	"os"
	"path/filepath"
	"strings"
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
	minioContainer, err := s3.NewMinioContainerFromConfig(config)
	require.NoError(suite.T(), err)
	suite.minioContainer = minioContainer

	err = suite.minioContainer.ClientWrapper.MakeDefaultBucket()
	require.NoError(suite.T(), err)

}

func (s *RootCliSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

func (suite *RootCliSuite) TestRootCmdWithTracing() {
	t := suite.T()

	t.Setenv("NABU_PROFILING", "true")

	// make sure that the trace file is created if we specify the cli arg even if the env var is not set
	args := []string{"test", "--cfg", filepath.Join(projectpath.Root, "config", "iow", "nabuconfig.yaml"), "--address", suite.minioContainer.Hostname, "--port", fmt.Sprint(suite.minioContainer.APIPort), suite.minioContainer.Hostname}
	rootCmd.SetArgs(args)
	Execute()
	_, err := os.Stat(filepath.Join(projectpath.Root, "trace.out"))
	require.NoError(t, err)
	defer os.Remove(filepath.Join(projectpath.Root, "trace.out"))

	_, err = os.Stat(filepath.Join(projectpath.Root, "http_trace.csv"))
	require.NoError(t, err)
	defer os.Remove(filepath.Join(projectpath.Root, "http_trace.csv"))

	objs, err := suite.minioContainer.ClientWrapper.ObjectList("traces/")
	require.NoError(t, err)
	require.Len(t, objs, 2)

	for _, obj := range objs {
		if !strings.Contains(obj.Key, ".csv") && !strings.Contains(obj.Key, ".out") {
			t.Fatal("not a trace file", obj.Key)
		}

		// clean up
		err = suite.minioContainer.ClientWrapper.Client.RemoveObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, obj.Key, minio.RemoveObjectOptions{})
		require.NoError(t, err)
	}

}

// Run the entire test suite
func TestRootClientSuite(t *testing.T) {
	suite.Run(t, new(RootCliSuite))
}
