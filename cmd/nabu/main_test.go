// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/common/projectpath"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestDefaultArgs(t *testing.T) {
	// Test the default args
	defaultRunner := NewNabuRunner([]string{"test"})
	require.Equal(t, "127.0.0.1", defaultRunner.args.Address)
	require.Equal(t, 9000, defaultRunner.args.Port)
	require.Equal(t, 1, defaultRunner.args.UpsertBatchSize)
}

func TestSubcommand(t *testing.T) {
	// Test the subcommand args to make sure that the subcommand is set properly
	defaultRunner := NewNabuRunner([]string{"object", "test", "--address", "DUMMY"})
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{})
	_, err := defaultRunner.Run(context.Background(), mockedClient)
	require.ErrorContains(t, err, "dial tcp: lookup")
	subCommandErr := strings.Contains(err.Error(), "subcommand 'object' requires a positional argument")
	require.False(t, subCommandErr)
}

// Wrapper struct to store a handle to the container for all
type RootCliSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

func (suite *RootCliSuite) SetupSuite() {
	minioContainer, err := s3.NewDefaultMinioContainer()
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer
}

func (s *RootCliSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

func (suite *RootCliSuite) TestRootCmdWithTracing() {

	// make sure that the trace file is created if we specify the cli arg even if the env var is not set
	args := []string{"test", "--trace", "--address", suite.minioContainer.Hostname, "--port",
		fmt.Sprint(suite.minioContainer.APIPort), "--bucket", suite.minioContainer.ClientWrapper.DefaultBucket,
		"--s3-access-key", "minioadmin", "--s3-secret-key", "minioadmin"}

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{})

	_, err := NewNabuRunner(args).Run(context.Background(), mockedClient)
	t := suite.T()
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(projectpath.Root, "trace.out"))
	require.NoError(t, err)
	defer func() {
		err := os.Remove(filepath.Join(projectpath.Root, "trace.out"))
		require.NoError(t, err)
	}()

	objs, err := suite.minioContainer.ClientWrapper.ObjectList(context.Background(), "traces/")

	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Contains(t, objs[0].Key, "trace")
	require.Contains(t, objs[0].Key, ".out")

	err = suite.minioContainer.ClientWrapper.Client.RemoveObject(context.Background(), suite.minioContainer.ClientWrapper.DefaultBucket, objs[0].Key, minio.RemoveObjectOptions{})
	require.NoError(t, err)

}

// Run the entire test suite
func TestRootClientSuite(t *testing.T) {
	suite.Run(t, new(RootCliSuite))
}
