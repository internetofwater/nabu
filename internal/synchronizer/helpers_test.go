package synchronizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindMissing(t *testing.T) {
	a := []string{"a", "b", "c", "d"}
	b := []string{"a", "c", "e"}
	res := findMissing(a, b)
	require.Equal(t, []string{"b", "d"}, res)
}

func TestDifference(t *testing.T) {
	// Test for difference function
	a := []string{"a", "b", "c", "d"}
	b := []string{"a", "c", "e", "f", "g"}
	res := difference(a, b)
	require.Equal(t, []string{"b", "d"}, res)
}

func TestGetTextBeforeDot(t *testing.T) {
	res := getTextBeforeDot("test.go")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test")
	require.Equal(t, "test", res)

	res = getTextBeforeDot("test.go.go")
	require.Equal(t, "test.go", res)
}
