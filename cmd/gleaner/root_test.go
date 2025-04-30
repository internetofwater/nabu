// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"nabu/internal/synchronizer/s3"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func startMocks() {
	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")
}

func (s *GleanerRootSuite) TestHarvestToS3() {
	startMocks()
	args := fmt.Sprintf("gleaner --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	err := NewGleanerRunner(strings.Split(args, " ")).Run()
	require.NoError(s.T(), err)
	objs, err := s.minioContainer.ClientWrapper.ObjectList("summoned/")
	require.NoError(s.T(), err)
	require.Len(s.T(), objs, 3)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	require.NoError(s.T(), err)
	require.Equal(s.T(), orgsObjs, 1)
}

func (s *GleanerRootSuite) TestHarvestToDisk() {
	startMocks()
	args := "gleaner --log-level DEBUG --to-disk --sitemap-index testdata/sitemap_index.xml"
	err := NewGleanerRunner(strings.Split(args, " ")).Run()
	require.NoError(s.T(), err)
}

// Wrapper struct to store a handle to the container for all
type GleanerRootSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

// Setup common dependencies before starting the test suite
func (suite *GleanerRootSuite) SetupSuite() {
	container, err := s3.NewDefaultMinioContainer()
	require.NoError(suite.T(), err)
	suite.minioContainer = container

}

func (s *GleanerRootSuite) TearDownSuite() {
	defer gock.Off()
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	require.NoError(s.T(), err)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(GleanerRootSuite))
}
