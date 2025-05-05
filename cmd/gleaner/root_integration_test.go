// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"nabu/internal/opentelemetry"
	"nabu/internal/synchronizer"
	"nabu/internal/synchronizer/s3"
	"nabu/internal/synchronizer/triplestore"
	"testing"
	"time"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/network"
)

// Wrapper struct to store a handle to the container for all
type GleanerInterationSuite struct {
	suite.Suite
	minioContainer   s3.MinioContainer
	graphdbContainer triplestore.GraphDBContainer
}

func (s *GleanerInterationSuite) TestIntegrationWithNabu() {
	s.T().Setenv("NABU_PROFILING", "False")
	startMocks()
	// need to enable networking to make graph http requests
	gock.EnableNetworking()
	defer gock.DisableNetworking()

	opentelemetry.InitTracer("gleaner_integration_test", opentelemetry.DefaultCollectorEndpoint)
	defer opentelemetry.Shutdown()

	args := fmt.Sprintf("--log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)

	span, ctx := opentelemetry.NewSpanAndContextWithName("gleaner_nabu_integration_test_sync_graphs")
	defer span.End()

	err := NewGleanerRunnerFromString(args).Run(ctx)
	require.NoError(s.T(), err)

	client, err := synchronizer.NewSynchronizerClientFromClients(
		&s.graphdbContainer.Client,
		s.minioContainer.ClientWrapper,
		s.minioContainer.ClientWrapper.DefaultBucket,
	)

	require.NoError(s.T(), err)

	err = client.SyncTriplestoreGraphs(ctx, "summoned/", true)
	require.NoError(s.T(), err)

	exists, err := client.GraphClient.GraphExists("urn:iow:summoned:stations__5:b38dced1575a8a83c1f5091c7de0b653.jsonld")
	require.NoError(s.T(), err)
	require.True(s.T(), exists)
}

func (suite *GleanerInterationSuite) SetupSuite() {

	ctx := context.Background()
	t := suite.T()
	net, err := network.New(ctx)
	require.NoError(t, err)
	minioContainer, err := s3.NewMinioContainerFromConfig(s3.MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
		ContainerName: "integrationTestMinio",
		Network:       net.Name,
	})
	suite.Require().NoError(err)
	suite.minioContainer = minioContainer

	stopHealthCheck, err := suite.minioContainer.ClientWrapper.Client.HealthCheck(5 * time.Second)
	require.NoError(t, err)
	defer stopHealthCheck()
	require.Eventually(t, func() bool {
		return suite.minioContainer.ClientWrapper.Client.IsOnline()
	}, 10*time.Second, 500*time.Millisecond, "MinIO container did not become online in time")

	err = suite.minioContainer.ClientWrapper.MakeDefaultBucket()
	require.NoError(t, err)

	graphdbContainer, err := triplestore.NewGraphDBContainer("iow", "./testdata/iow-config.ttl")
	suite.Require().NoError(err)
	suite.graphdbContainer = graphdbContainer
}

func (s *GleanerInterationSuite) TearDownSuite() {
	defer gock.Off()
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

// Run the entire test suite
func TestGleanerIntegrationClientSuite(t *testing.T) {
	suite.Run(t, new(GleanerInterationSuite))
}
