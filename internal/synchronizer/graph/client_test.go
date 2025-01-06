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

func (suite *GraphDbClientSuite) TestGraphExists() {
	t := suite.T()
	isGraph, err := suite.graphdb.Client.GraphExists("http://example.org/DUMMY_GRAPH")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

	// try a malformed query, make sure it errors
	_, err = suite.graphdb.Client.GraphExists("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MALFORMED QUERY")
}

func (suite *GraphDbClientSuite) TestInsert() {
	graph := "http://example.org/graph/test"
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .
	<http://example.org/resource/2> <http://example.org/property/name> "Bob" .
`
	t := suite.T()

	err := suite.graphdb.Client.InsertWithNamedGraph(data, graph)
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(graph)
	require.NoError(t, err)
	require.True(t, graphExists)
}

func (suite *GraphDbClientSuite) TestDropGraph() {

	graph := "http://example.org/graph/test"
	t := suite.T()

	// insert data with the graph
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .`

	err := suite.graphdb.Client.InsertWithNamedGraph(data, graph)
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(graph)
	require.NoError(t, err)
	require.True(t, graphExists)

	err = suite.graphdb.Client.DropGraph(graph)
	require.NoError(t, err)

	graphExists, err = suite.graphdb.Client.GraphExists(graph)
	require.NoError(t, err)
	require.False(t, graphExists)
}

// Run the entire test suite
func TestGraphdbTestSuite(t *testing.T) {
	suite.Run(t, new(GraphDbClientSuite))
}
