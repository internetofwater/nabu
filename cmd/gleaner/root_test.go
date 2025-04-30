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

func (s *GleanerRootSuite) TestRunOnSitemapIndex() {

	args := fmt.Sprintf("gleaner --log-level DEBUG --sitemap-index https://geoconnex.us/sitemap.xml --address %s --port %d --bucket %s", s.minioContainer.Hostname, s.minioContainer.APIPort, s.minioContainer.ClientWrapper.DefaultBucket)
	err := NewGleanerRunner(strings.Split(args, " ")).Run()
	require.NoError(s.T(), err)
}

func (s *GleanerRootSuite) TestRunOnSitemapIndexWithLocalFS() {
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

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")
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
