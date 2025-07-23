// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func startMocks() {
	gock.EnableNetworking()
	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml").Mock.Request().Persist()

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml").Mock.Request().Persist()

}

func NewNabuRunnerFromString(args string) NabuRunner {
	return NewNabuRunner(strings.Split(args, " "))
}

func (s *GleanerRootSuite) TestHarvestToS3() {
	startMocks()
	defer gock.Off()
	args := fmt.Sprintf("harvest --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	_, err := NewNabuRunnerFromString(args).Run(context.Background())
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
	defer gock.Off()
	args := fmt.Sprintf("harvest --log-level DEBUG --sitemap-index testdata/sitemap_index.xml --source iow_wqp_stations__5 --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	_, err := NewNabuRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	s.Require().NoError(err)
	require.Equal(s.T(), 1, orgsObjs)
}

func (s *GleanerRootSuite) TestHarvestToDisk() {
	startMocks()
	defer gock.Off()
	args := "harvest --log-level DEBUG --to-disk --sitemap-index testdata/sitemap_index.xml"
	_, err := NewNabuRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)
}

func startMocksForBadFileType() {
	gock.EnableNetworking()

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index_selfie.xml").Mock.Request().Persist()

	gock.New("https://geoconnex.us/sitemap/SELFIE/SELFIE_ids__0.xml").
		Reply(200).
		File("testdata/SELFIE_ids__0.xml").Mock.Request().Persist()

	gock.New("https://geoconnex.us/SELFIE/usgs/huc/huc12obs/070900020601").
		Reply(200).
		File("testdata/selfie.html").Mock.Request().Persist()
}

func (s *GleanerRootSuite) TestBadFileType() {
	startMocksForBadFileType()
	defer gock.Off()
	args := "harvest --sitemap-index https://geoconnex.us/sitemap.xml --source SELFIE_SELFIE_ids__0 --log-level DEBUG --to-disk"
	stats, err := NewNabuRunnerFromString(args).Run(context.Background())
	s.Require().NoError(err)
	s.Require().Len(stats, 1)
	s.Require().Len(stats[0].CrawlFailures, 0)
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
