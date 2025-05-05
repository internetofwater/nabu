// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

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

func TestDefaultArgs(t *testing.T) {
	// Test the default args
	defaultRunner := NewNabuRunner([]string{"test"})
	require.Equal(t, "minio", defaultRunner.args.Address)
	require.Equal(t, 9000, defaultRunner.args.Port)
	require.Equal(t, 1, defaultRunner.args.UpsertBatchSize)
}

func TestSubcommand(t *testing.T) {
	// Test the subcommand args to make sure that the subcommand is set properly
	defaultRunner := NewNabuRunner([]string{"object", "test", "--address", "DUMMY"})
	err := defaultRunner.Run(context.Background())
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
	require.NoError(suite.T(), err)
	suite.minioContainer = minioContainer
}

func (s *RootCliSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

func (suite *RootCliSuite) TestRootCmdWithTracing() {

	// make sure that the trace file is created if we specify the cli arg even if the env var is not set
	args := []string{"test", "--trace", "--address", suite.minioContainer.Hostname, "--port",
		fmt.Sprint(suite.minioContainer.APIPort), "--bucket", suite.minioContainer.ClientWrapper.DefaultBucket,
		"--s3-access-key", "minioadmin", "--s3-secret-key", "minioadmin"}

	err := NewNabuRunner(args).Run(context.Background())
	t := suite.T()
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(projectpath.Root, "trace.out"))
	require.NoError(t, err)
	defer os.Remove(filepath.Join(projectpath.Root, "trace.out"))

	_, err = os.Stat(filepath.Join(projectpath.Root, "http_trace.csv"))
	require.NoError(t, err)
	defer os.Remove(filepath.Join(projectpath.Root, "http_trace.csv"))

	objs, err := suite.minioContainer.ClientWrapper.ObjectList(context.Background(), "traces/")

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
