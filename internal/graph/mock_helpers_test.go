package graph

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test the GraphDB container will spin up so our tests can use it
func TestGraphdbInit(t *testing.T) {

	graphdb, err := NewGraphDBContainer()
	require.NoError(t, err)
	err = (*graphdb.Container).Terminate(context.Background())
	require.NoError(t, err)
}
