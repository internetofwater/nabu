package graph

import (
	"nabu/testHelpers"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_IsGraph(t *testing.T) {
	graphdb, err := testHelpers.NewGraphDBContainer()
	require.NoError(t, err)
	isGraph, err := IsGraph(graphdb.FullEndpoint, "dummy")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

	isGraph, err = IsGraph(graphdb.FullEndpoint, "")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

}
