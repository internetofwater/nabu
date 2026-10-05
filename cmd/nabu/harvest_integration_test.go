// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/parquettable"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/synchronizer"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/network"
)

// Wrapper struct to store a handle to the container for all
type NabuInterationSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

func (s *NabuInterationSuite) TestIntegrationWithNabu() {

	opentelemetry.InitTracer("harvest_integration_test", opentelemetry.DefaultTracingEndpoint)
	defer opentelemetry.Shutdown()

	args := fmt.Sprintf("harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)

	ctx, span := opentelemetry.NewSpanAndContextWithName("gleaner_nabu_integration_test_sync_graphs")
	defer span.End()

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                     {File: "testdata/sitemap_index.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/robots.txt":                      {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})
	_, err := NewNabuRunnerFromString(args).Run(ctx, mockedClient)
	s.Require().NoError(err)

	client, err := synchronizer.NewSynchronizerClientFromClients(
		s.minioContainer.ClientWrapper,
		s.minioContainer.ClientWrapper.DefaultBucket,
		s.minioContainer.ClientWrapper.MetadataBucket,
	)

	s.Require().NoError(err)

	const pid = "https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"

	parquetData, err := client.S3Client.GetObjectAsBytes(crawl.SummonedParquetPath("iow:wqp:stations__5"))
	s.Require().NoError(err)
	s.Require().True(len(parquetData) > 0, "parquet file should not be empty")

	rows := 0
	err = parquettable.Read(ctx, bytes.NewReader(parquetData), func(f parquettable.Feature) error {
		rows++
		if f.URL == pid {
			s.Require().Contains(string(f.JSONLD), pid, "jsonld should contain the original pid")
		}
		return nil
	})
	s.Require().NoError(err)
	s.Require().Equal(2, rows)

	nquadsArgs := fmt.Sprintf("nquads --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	runner := NewNabuRunnerFromString(nquadsArgs)
	var stdout bytes.Buffer
	runner.stdout = &stdout
	_, err = runner.Run(ctx, mockedClient)
	s.Require().NoError(err)

	encodedPid := base64.StdEncoding.EncodeToString([]byte(pid))
	s.Require().Equal("aHR0cHM6Ly9nZW9jb25uZXgudXMvaW93L3dxcC9CUE1XUVgtMTA4NC1XUi1DQzAxQw==", encodedPid)
	s.Require().Contains(stdout.String(), "<"+pid+">", "nq output should contain the original pid")
	s.Require().Contains(stdout.String(), "<urn:iow:summoned:iow:wqp:stations__5:"+encodedPid+".jsonld>", "the graph name should be the same as in previous nq releases")
}

func (suite *NabuInterationSuite) SetupSuite() {

	ctx := context.Background()
	t := suite.T()
	net, err := network.New(ctx)
	require.NoError(t, err)
	minioContainer, err := s3.NewMinioContainerFromConfig(s3.MinioContainerConfig{
		Username:       "minioadmin",
		Password:       "minioadmin",
		DefaultBucket:  "iow",
		MetadataBucket: "metadata",
		ContainerName:  "integrationTestMinio",
		Network:        net.Name,
	})
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	stopHealthCheck, err := suite.minioContainer.ClientWrapper.Client.HealthCheck(5 * time.Second)
	require.NoError(t, err)
	defer stopHealthCheck()
	require.Eventually(t, func() bool {
		return suite.minioContainer.ClientWrapper.Client.IsOnline()
	}, 10*time.Second, 500*time.Millisecond, "MinIO container did not become online in time")

	err = suite.minioContainer.ClientWrapper.SetupBuckets()
	require.NoError(t, err)
}

func (s *NabuInterationSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestNabuIntegrationClientSuite(t *testing.T) {
	suite.Run(t, new(NabuInterationSuite))
}
