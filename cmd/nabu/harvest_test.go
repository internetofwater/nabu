// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/minio/minio-go/v7"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func NewNabuRunnerFromString(args string) NabuRunner {
	return NewNabuRunner(strings.Split(args, " "))
}

func (s *GleanerRootSuite) TestHarvestToS3() {
	args := fmt.Sprintf("harvest --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                     {File: "testdata/sitemap_index.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
	})
	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
	objs, err := s.minioContainer.ClientWrapper.ObjectList(context.Background(), "summoned/")
	s.Require().NoError(err)
	// two jsonld objects
	s.Require().Len(objs, 2)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	s.Require().NoError(err)
	// they come from the same org so should be 1
	require.Equal(s.T(), 1, orgsObjs)

	// access all the objects in the metadata bucket and make sure they exist
	buckets, err := s.minioContainer.ClientWrapper.Client.ListBuckets(context.Background())
	require.NoError(s.T(), err)
	const harvestBucketAndMetadataBucket = 2
	require.Len(s.T(), buckets, harvestBucketAndMetadataBucket)

	var metadataBucket minio.BucketInfo
	found := false
	for _, bucket := range buckets {
		if bucket.Name != s.minioContainer.ClientWrapper.MetadataBucket {
			continue
		}

		metadataBucket = bucket
		found = true
	}
	require.True(s.T(), found)
	objChan := s.minioContainer.ClientWrapper.Client.ListObjects(context.Background(), metadataBucket.Name, minio.ListObjectsOptions{Recursive: true})
	items := 0
	for obj := range objChan {
		require.NoError(s.T(), obj.Err)
		require.NotEmpty(s.T(), obj.Key)
		items++
	}
	const OneSitemapSoOneMetadataObject = 1
	require.Equal(s.T(), OneSitemapSoOneMetadataObject, items)
}

func (s *GleanerRootSuite) TestHarvestWithSourceSpecified() {
	args := fmt.Sprintf("harvest --log-level DEBUG --sitemap-index testdata/sitemap_index.xml --source iow_wqp_stations__5 --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
	})

	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)

	orgsObjs, err := s.minioContainer.ClientWrapper.NumberOfMatchingObjects([]string{"orgs/"})
	s.Require().NoError(err)
	require.Equal(s.T(), 1, orgsObjs)
}

func (s *GleanerRootSuite) TestHarvestToDisk() {
	args := "harvest --log-level DEBUG --to-disk --sitemap-index testdata/sitemap_index.xml"
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                     {File: "testdata/sitemap_index.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
	})
	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
}

func (s *GleanerRootSuite) TestBadFileType() {
	args := "harvest --sitemap-index https://geoconnex.us/sitemap.xml --source SELFIE_SELFIE_ids__0 --log-level DEBUG --to-disk"
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                           {File: "testdata/sitemap_index_selfie.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/SELFIE/SELFIE_ids__0.xml":      {File: "testdata/SELFIE_ids__0.xml", StatusCode: 200},
		"https://geoconnex.us/SELFIE/usgs/huc/huc12obs/070900020601": {Body: "DUMMY BAD", StatusCode: 200, ContentType: "application/ld+DUMMY"},
	})
	stats, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
	s.Require().Len(stats, 1)
	s.Require().Len(stats[0].CrawlFailures, 1)
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
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(GleanerRootSuite))
}
