package triplestore

import (
	"nabu/internal/common"
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
	graphdb, err := NewGraphDBContainer("iow", "./testdata/iow-config.ttl")
	require.NoError(suite.T(), err)
	suite.graphdb = graphdb
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

	err := suite.graphdb.Client.InsertNamedGraphs([]common.NamedGraph{{GraphURI: graph, Triples: data}})
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(graph)
	require.NoError(t, err)
	require.True(t, graphExists)

	bad_data := `
	<http://example.org/resource/1> .`

	err = suite.graphdb.Client.InsertNamedGraphs([]common.NamedGraph{{GraphURI: graph, Triples: bad_data}})
	require.Error(t, err)

}

func (suite *GraphDbClientSuite) TestDropGraphs() {
	t := suite.T()

	graph1 := "http://example.org/graph/test"

	// insert data with the graph
	data := `
	<http://example.org/resource/1> <http://example.org/property/name> "Alice" .`

	err := suite.graphdb.Client.InsertNamedGraphs([]common.NamedGraph{{GraphURI: graph1, Triples: data}})
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(graph1)
	require.NoError(t, err)
	require.True(t, graphExists)

	graph2 := "http://example.org/graph/test2"
	err = suite.graphdb.Client.InsertNamedGraphs([]common.NamedGraph{{GraphURI: graph2, Triples: data}})
	require.NoError(t, err)
	graphExists, err = suite.graphdb.Client.GraphExists(graph2)
	require.NoError(t, err)
	require.True(t, graphExists)

	err = suite.graphdb.Client.DropGraphs([]string{graph1, graph2})
	require.NoError(t, err)

	graphExists, err = suite.graphdb.Client.GraphExists(graph1)
	require.NoError(t, err)
	require.False(t, graphExists)

	graphExists, err = suite.graphdb.Client.GraphExists(graph2)
	require.NoError(t, err)
	require.False(t, graphExists)

	err = suite.graphdb.Client.DropGraphs([]string{})
	require.Error(t, err)
}

func (suite *GraphDbClientSuite) TestClearAll() {
	err := suite.graphdb.Client.ClearAllGraphs()
	require.NoError(suite.T(), err)
}

// Run the entire test suite
func TestGraphdbTestSuite(t *testing.T) {
	suite.Run(t, new(GraphDbClientSuite))
}
