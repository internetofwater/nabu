// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/pkg"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func NewNabuRunnerFromString(args string) NabuRunner {
	return NewNabuRunner(strings.Split(args, " "))
}

func (s *NabuHarvestSuite) TestHarvestToS3() {
	args := fmt.Sprintf("harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                     {File: "testdata/sitemap_index.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/robots.txt":                      {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})
	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
	objs, err := s.minioContainer.ClientWrapper.ObjectList(context.Background(), "summoned/")
	s.Require().NoError(err)
	// one parquet file containing both jsonld documents
	s.Require().Len(objs, 1)
	s.Require().Equal("summoned/iow:wqp:stations__5.parquet", objs[0].Key)

	// the crawl stats are stored in the footer of the parquet file
	stored, err := s.minioContainer.ClientWrapper.Get(objs[0].Key)
	s.Require().NoError(err)
	defer func() { _ = stored.Close() }()
	readerAtSeeker, err := parquettable.AsReaderAtSeeker(stored)
	s.Require().NoError(err)
	statsJson, ok, err := parquettable.ReadKeyValueMetadata(readerAtSeeker, crawl.CrawlStatsMetadataKey)
	s.Require().NoError(err)
	s.Require().True(ok, "the parquet file should have the crawl stats in its footer")
	var stats pkg.SitemapCrawlStats
	s.Require().NoError(json.Unmarshal([]byte(statsJson), &stats))
	s.Require().Equal("iow:wqp:stations__5", stats.SitemapName)
	s.Require().Equal(2, stats.SuccessfulSites)
}

func (s *NabuHarvestSuite) TestHarvestWithSourceSpecified() {
	args := fmt.Sprintf("harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --log-level DEBUG --sitemap-index testdata/sitemap_index.xml --source iow:wqp:stations__5 --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/robots.txt":                      {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})

	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
}

func (s *NabuHarvestSuite) TestHarvestToDisk() {
	args := "harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --log-level DEBUG --to-disk --sitemap-index testdata/sitemap_index.xml"
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                     {File: "testdata/sitemap_index.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/robots.txt":                      {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})
	_, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
}

func (s *NabuHarvestSuite) TestBadFileType() {
	args := "harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --sitemap-index https://geoconnex.us/sitemap.xml --source SELFIE:ids__0 --log-level DEBUG --to-disk"
	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml":                           {File: "testdata/sitemap_index_selfie.xml", StatusCode: 200},
		"https://geoconnex.us/sitemap/SELFIE/SELFIE_ids__0.xml":      {File: "testdata/SELFIE_ids__0.xml", StatusCode: 200},
		"https://geoconnex.us/SELFIE/usgs/huc/huc12obs/070900020601": {Body: "DUMMY BAD", StatusCode: 200, ContentType: "application/ld+DUMMY"},
		"https://geoconnex.us/robots.txt":                            {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})
	stats, err := NewNabuRunnerFromString(args).Run(context.Background(), mockedClient)
	s.Require().NoError(err)
	s.Require().Len(stats, 1)
	s.Require().Len(stats[0].CrawlFailures, 1)
}

// Wrapper struct to store a handle to the container for all
type NabuHarvestSuite struct {
	suite.Suite
	minioContainer s3.MinioContainer
}

// Setup common dependencies before starting the test suite
func (suite *NabuHarvestSuite) SetupSuite() {
	container, err := s3.NewDefaultMinioContainer()
	suite.Require().NoError(err)
	suite.minioContainer = container

}

func (s *NabuHarvestSuite) TearDownSuite() {
	c := *s.minioContainer.Container
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestS3ClientSuite(t *testing.T) {
	suite.Run(t, new(NabuHarvestSuite))
}

func harvestTestClient() *http.Client {
	return common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {File: "testdata/stations__5.xml", StatusCode: 200},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2":   {File: "testdata/1085.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C":    {File: "testdata/1084.jsonld", StatusCode: 200, ContentType: "application/ld+json"},
		"https://geoconnex.us/robots.txt":                      {File: "testdata/geoconnex_robots.txt", StatusCode: 200, ContentType: "application/text/plain"},
	})
}

func TestHarvestWithLocalShacl(t *testing.T) {
	args := "harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --to-disk --shacl-local --sitemap-index testdata/sitemap_index.xml"
	stats, err := NewNabuRunnerFromString(args).Run(context.Background(), harvestTestClient())
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Equal(t, 2, stats[0].SuccessfulSites)
	// both test documents are missing a name so both should be flagged by the local validator
	require.Len(t, stats[0].WarningStats.ShaclWarnings, 2)
	for _, warning := range stats[0].WarningStats.ShaclWarnings {
		// every warning must be a shacl result rather than an issue running validation
		require.Equal(t, pkg.ShaclInvalid, warning.ShaclStatus)
		require.NotContains(t, warning.ShaclValidationMessage, "failed to communicate")
	}
}

func TestHarvestWithLocalAndGrpcShaclFails(t *testing.T) {
	args := "harvest --mainstem-metadata ../../internal/mainstems/testdata/colorado_subset.fgb --to-disk --shacl-local --shacl-grpc-endpoint 0.0.0.0:50051 --sitemap-index testdata/sitemap_index.xml"
	_, err := NewNabuRunnerFromString(args).Run(context.Background(), harvestTestClient())
	require.Error(t, err)
}

func TestHarvestMainstemFileIsOptional(t *testing.T) {
	t.Run("not providing a mainstem file is an error if a sitemap requests mainstems", func(t *testing.T) {
		// this sitemap index sets add_associated_mainstems
		args := "harvest --to-disk --sitemap-index testdata/sitemap_index.xml"
		_, err := NewNabuRunnerFromString(args).Run(context.Background(), harvestTestClient())
		require.ErrorContains(t, err, "but no mainstem file was provided")
	})
	t.Run("a mainstem file that does not exist is an error", func(t *testing.T) {
		args := "harvest --to-disk --mainstem-metadata does_not_exist.fgb --sitemap-index testdata/sitemap_index.xml"
		_, err := NewNabuRunnerFromString(args).Run(context.Background(), harvestTestClient())
		require.ErrorContains(t, err, "does not exist locally")
	})
}
