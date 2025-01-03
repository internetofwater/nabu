package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type GraphDbClientSuite struct {
	suite.Suite
	graphdb GraphDBContainer
}

// Setup common dependencies before starting the test suite
func (suite *GraphDbClientSuite) SetupSuite() {
	graphdb, err := NewGraphDBContainer("iow")
	require.NoError(suite.T(), err)
	suite.graphdb = graphdb
	t := suite.T()
	configPath := "./test_data/iow-config.ttl"
	err = suite.graphdb.Client.CreateRepository(configPath)
	require.NoError(t, err)
}

func (suite *GraphDbClientSuite) Test_GraphExists() {
	t := suite.T()
	isGraph, err := suite.graphdb.Client.GraphExists("http://example.org/graph/test")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

	// try a malformed query, make sure it errors
	_, err = suite.graphdb.Client.GraphExists("")
	require.Error(t, err)

}

func (suite *GraphDbClientSuite) TestCreateGraph() {
	t := suite.T()

	client := suite.graphdb.Client

	testGraphName := "http://example.org/graph/test"

	err := client.CreateGraph(testGraphName)
	require.NoError(t, err)

	res, err := client.GraphExists("dummyGraph")
	require.NoError(t, err)
	require.True(t, res)

}

// Run the entire test suite
func TestGraphdbTestSuite(t *testing.T) {
	suite.Run(t, new(GraphDbClientSuite))
}
