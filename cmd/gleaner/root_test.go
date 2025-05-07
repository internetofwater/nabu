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

func NewGleanerRunnerFromString(args string) GleanerRunner {
	return NewGleanerRunner(strings.Split(args, " "))
}

func (s *GleanerRootSuite) TestHarvestToS3() {
	startMocks()
	args := fmt.Sprintf("--log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	_, err := NewGleanerRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)
	objs, err := s.minioContainer.ClientWrapper.ObjectList(context.Background(), "summoned/")
	s.Require().NoError(err)
	s.Require().Len(objs, 3)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	s.Require().NoError(err)
	require.Equal(s.T(), 1, orgsObjs)
}

func (s *GleanerRootSuite) TestHarvestWithSourceSpecified() {
	startMocks()
	args := fmt.Sprintf("--log-level DEBUG --sitemap-index testdata/sitemap_index.xml --source iow_wqp_stations__5 --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	_, err := NewGleanerRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	s.Require().NoError(err)
	require.Equal(s.T(), 1, orgsObjs)
}

func (s *GleanerRootSuite) TestHarvestToDisk() {
	startMocks()
	args := "--log-level DEBUG --to-disk --sitemap-index testdata/sitemap_index.xml"
	_, err := NewGleanerRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)
}

func startMocksForBadFileType() {
	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index_selfie.xml")

	gock.New("https://geoconnex.us/sitemap/SELFIE/SELFIE_ids__0.xml").
		Reply(200).
		File("testdata/SELFIE_ids__0.xml")

	gock.New("https://geoconnex.us/SELFIE/usgs/huc/huc12obs/070900020601").
		Reply(200).
		File("testdata/selfie.html")
}

func (s *GleanerRootSuite) TestBadFileType() {
	startMocksForBadFileType()
	args := "--sitemap-index https://geoconnex.us/sitemap.xml --source SELFIE_SELFIE_ids__0"
	stats, err := NewGleanerRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)
	crawlError := stats[0].CrawlFailures[0]
	require.Equal(s.T(), len(stats[0].CrawlFailures), 1)
	require.Equal(s.T(), crawlError.Status, 200)
}

// Wrapper struct to store a handle to the container for all
type GleanerRootSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

// Setup common dependencies before starting the test suite
func (suite *GleanerRootSuite) SetupSuite() {
	container, err := s3.NewDefaultMinioContainer()
	suite.Require().NoError(err)
	suite.minioContainer = container

}

func (s *GleanerRootSuite) TearDownSuite() {
	defer gock.Off()
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(GleanerRootSuite))
}
