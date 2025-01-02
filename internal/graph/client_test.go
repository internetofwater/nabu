package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GraphExists(t *testing.T) {
	graphdb, err := NewGraphDBContainer()
	require.NoError(t, err)
	isGraph, err := graphdb.Client.GraphExists("dummy")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

	isGraph, err = graphdb.Client.GraphExists("")

	require.Equal(t, false, isGraph)
	require.NoError(t, err)

}
