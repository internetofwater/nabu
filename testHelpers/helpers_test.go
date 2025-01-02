package testHelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGraphdbInit(t *testing.T) {

	graphdb, err := NewGraphDBContainer()
	require.NoError(t, err)
	defer (*graphdb.Container).Terminate(context.Background())
}
