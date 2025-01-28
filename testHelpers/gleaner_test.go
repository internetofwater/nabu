package testhelpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGleanerContainerVersion(t *testing.T) {
	gleaner, err := NewGleanerExecutor()
	require.NoError(t, err)
	result, err := gleaner.Run("--help")
	require.NoError(t, err)
	require.Contains(t, result, "Gleaner")
}
