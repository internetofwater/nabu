package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeUrn(t *testing.T) {
	t.Run("prefix with no slashes fails", func(t *testing.T) {
		const shortPrefixWithNoSlashes = "test"
		_, err := MakeURN(shortPrefixWithNoSlashes)
		require.Error(t, err)
	})

	t.Run("prefix with 3 parts preserves all three but changes order", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3")
		require.NoError(t, err)
		require.Equal(t, "urn:gleaner.io:iow:test2:test1:test3", result)
	})

	t.Run("prefix with more than 4 parts drops one", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3/test4")
		require.NoError(t, err)
		require.Equal(t, "urn:gleaner.io:iow:test3:test2:test4", result)
	})
}

func TestMakeUrnPrefix(t *testing.T) {
	t.Run("prefix with no slashes fails", func(t *testing.T) {
		const shortPrefixWithNoSlashes = "test"
		_, err := MakeURNFromS3Prefix(shortPrefixWithNoSlashes)
		require.Error(t, err)
	})

	t.Run("prefix with >= 3 reverses order and drops one", func(t *testing.T) {
		result, err := MakeURNFromS3Prefix("test1/test2/test3/test4")
		require.NoError(t, err)
		regularUrn, _ := MakeURN("test1/test2/test3/test3")
		require.NotEqual(t, regularUrn, result)
		require.Equal(t, "urn:gleaner.io:iow:test4:test3", result)
	})
}
