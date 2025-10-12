// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/synchronizer"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/internal/synchronizer/triplestores"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/network"
)

// Wrapper struct to store a handle to the container for all
type GleanerInterationSuite struct {
	suite.Suite
	minioContainer   s3.MinioContainer
	graphdbContainer triplestores.GraphDBContainer
}

func (s *GleanerInterationSuite) TestIntegrationWithNabu() {

	opentelemetry.InitTracer("harvest_integration_test", opentelemetry.DefaultTracingEndpoint)
	defer opentelemetry.Shutdown()

	args := fmt.Sprintf("harvest --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)

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
		&s.graphdbContainer.Client,
		s.minioContainer.ClientWrapper,
		s.minioContainer.ClientWrapper.DefaultBucket,
		s.minioContainer.ClientWrapper.MetadataBucket,
	)

	s.Require().NoError(err)

	err = client.SyncTriplestoreGraphs(ctx, "summoned/", true)
	s.Require().NoError(err)

	const pid = "https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C"
	encodedPid := base64.StdEncoding.EncodeToString([]byte(pid))
	s.Require().Equal("aHR0cHM6Ly9nZW9jb25uZXgudXMvaW93L3dxcC9CUE1XUVgtMTA4NC1XUi1DQzAxQw==", encodedPid)
	exists, err := client.GraphClient.GraphExists(context.Background(), "urn:iow:summoned:iow_wqp_stations__5:"+encodedPid+".jsonld")
	s.Require().NoError(err)
	s.Require().True(exists)
}

func (suite *GleanerInterationSuite) SetupSuite() {

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

	graphdbContainer, err := triplestores.NewGraphDBContainer("iow", "./testdata/iow-config.ttl")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer
}

func (s *GleanerInterationSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestGleanerIntegrationClientSuite(t *testing.T) {
	suite.Run(t, new(GleanerInterationSuite))
}
