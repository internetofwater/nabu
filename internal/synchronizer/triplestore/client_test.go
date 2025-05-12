// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestore

import (
	"context"
	"testing"

	"github.com/internetofwater/nabu/internal/common"

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
	suite.Require().NoError(err)
	suite.graphdb = graphdb
}

func (suite *GraphDbClientSuite) TestGraphExists() {
	t := suite.T()
	isGraph, err := suite.graphdb.Client.GraphExists(context.Background(), "http://example.org/DUMMY_GRAPH")

	require.False(t, isGraph)
	require.NoError(t, err)

	// try a malformed query, make sure it errors
	_, err = suite.graphdb.Client.GraphExists(context.Background(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MALFORMED QUERY")
}

func (suite *GraphDbClientSuite) TestInsert() {
	graph := "urn://example.org/graph/test"
	data := `
	<urn://example.org/resource/1> <urn://example.org/property/name> "Alice" .
	<urn://example.org/resource/2> <urn://example.org/property/name> "Bob" .
`
	t := suite.T()

	err := suite.graphdb.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{{GraphURI: graph, Triples: data}})
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(context.Background(), graph)
	require.NoError(t, err)
	require.True(t, graphExists)

	bad_data := `
	<urn://example.org/resource/1> .`

	err = suite.graphdb.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{{GraphURI: graph, Triples: bad_data}})
	require.Error(t, err)

}

func (suite *GraphDbClientSuite) TestDropGraphs() {
	t := suite.T()

	graph1 := "urn://example.org/graph/test"

	// insert data with the graph
	data := `
	<urn://example.org/resource/1> <urn://example.org/property/name> "Alice" .`

	err := suite.graphdb.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{{GraphURI: graph1, Triples: data}})
	require.NoError(t, err)

	graphExists, err := suite.graphdb.Client.GraphExists(context.Background(), graph1)
	require.NoError(t, err)
	require.True(t, graphExists)

	graph2 := "urn://example.org/graph/test2"
	err = suite.graphdb.Client.UpsertNamedGraphs(context.Background(), []common.NamedGraph{{GraphURI: graph2, Triples: data}})
	require.NoError(t, err)
	graphExists, err = suite.graphdb.Client.GraphExists(context.Background(), graph2)
	require.NoError(t, err)
	require.True(t, graphExists)

	err = suite.graphdb.Client.DropGraphs(context.Background(), []string{graph1, graph2})
	require.NoError(t, err)

	graphExists, err = suite.graphdb.Client.GraphExists(context.Background(), graph1)
	require.NoError(t, err)
	require.False(t, graphExists)

	graphExists, err = suite.graphdb.Client.GraphExists(context.Background(), graph2)
	require.NoError(t, err)
	require.False(t, graphExists)

	err = suite.graphdb.Client.DropGraphs(context.Background(), []string{})
	require.Error(t, err)
}

func (suite *GraphDbClientSuite) TestClearAll() {
	err := suite.graphdb.Client.ClearAllGraphs()
	suite.Require().NoError(err)
}

// Run the entire test suite
func TestGraphdbTestSuite(t *testing.T) {
	suite.Run(t, new(GraphDbClientSuite))
}
