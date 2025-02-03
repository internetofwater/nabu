package synchronizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrphanAndMissing(t *testing.T) {

	s3MockNames := []string{"a", "b", "c", "d", "e"}
	tripleStoreMockNames := []string{"a", "b", "c", "g", "h"}

	missingToAdd := findMissing(s3MockNames, tripleStoreMockNames)
	require.Equal(t, []string{"d", "e"}, missingToAdd)

	orphaned := findMissing(tripleStoreMockNames, s3MockNames)
	require.Equal(t, []string{"g", "h"}, orphaned)
}

func TestGetTextBeforeDot(t *testing.T) {
	res := getTextBeforeDot("test.go")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test.go.go")
	require.Equal(t, "test.go", res)
}
