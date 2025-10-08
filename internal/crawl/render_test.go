// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"fmt"
	"os"
	"testing"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

const epaEndpoint = "https://mywaterway.epa.gov/community/101701010503/overview"

// Wrapper struct to store a handle to the container for all
type JSRenderSuite struct {
	suite.Suite
	jsRenderService testcontainers.Container
	client          SplashClient
}

// Spin up a local minio container
func NewSplashContainerFromConfig() (testcontainers.Container, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "chromedp/headless-shell:latest",
		ExposedPorts: []string{"9222/tcp"},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return nil, fmt.Errorf("generic container: %w", err)
	}

	return genericContainer, nil
}

func (suite *JSRenderSuite) SetupSuite() {
	container, err := NewSplashContainerFromConfig()
	suite.Require().NoError(err)
	suite.jsRenderService = container
	ctx := context.Background()
	connPort, err := container.MappedPort(ctx, "9222/tcp")
	suite.Require().NoError(err)
	mockedClient := common.NewMockedClient(false, map[string]common.MockResponse{
		epaEndpoint: {File: "testdata/epa.html", StatusCode: 200, ContentType: "text/html"},
	})
	splashClient := NewSplashClient("http://localhost:"+connPort.Port(), mockedClient)
	suite.client = *splashClient
}

func (s *JSRenderSuite) TestRenderEPA() {
	jsonld, err := s.client.RenderContent(context.Background(), epaEndpoint)
	s.Require().NoError(err)
	// write jsonld to file
	err = os.WriteFile("testdata/epa_splash.html", jsonld, 0644)
	s.Require().NoError(err)

	baseWithoutJS, err := os.Open("testdata/epa.html")
	s.Require().NoError(err)
	s.Require().NotEqual(jsonld, baseWithoutJS)
}

func (s *JSRenderSuite) TearDownSuite() {
	c := s.jsRenderService
	err := c.Terminate(context.Background())
	s.Require().NoError(err)
}

// Run the entire test suite
func TestJSSuite(t *testing.T) {
	suite.Run(t, new(JSRenderSuite))
}
