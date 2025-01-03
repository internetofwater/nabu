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

func (suite *GraphDbClientSuite) SetupSuite() {
	graphdb, err := NewGraphDBContainer()
	require.NoError(suite.T(), err)
	suite.graphdb = graphdb
}

func (suite *GraphDbClientSuite) Test_GraphExists() {
	t := suite.T()
	isGraph, err := suite.graphdb.Client.GraphExists("dummy")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

	isGraph, err = suite.graphdb.Client.GraphExists("")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

}

func (suite *GraphDbClientSuite) Test_Insert() {
	t := suite.T()

	client := suite.graphdb.Client

	testGraphName := "http://example.org/graph/test"

	err := client.CreateGraph(testGraphName)
	require.NoError(t, err)

	testData := "<http://example.org/resource/subject> <http://example.org/predicate/relation> \"Sample data\" ."
	err = client.Insert(testGraphName, testData, false)
	require.NoError(t, err)

	// res, err := client.GraphExists("dummyGraph")
	// require.NoError(t, err)
	// require.True(t, res)

}

func TestGraphdbTestSuite(t *testing.T) {
	suite.Run(t, new(GraphDbClientSuite))
}
