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
		require.Equal(t, "urn:iow:test1:test2:test3", result)
	})

	t.Run("prefix with more than 4 parts drops one", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3/test4")
		require.NoError(t, err)
		require.Equal(t, "urn:iow:test1:test2:test3:test4", result)
	})
	t.Run("prefix with two slashes fails", func(t *testing.T) {
		const prefixWithTwoSlashes = "test1//test2"
		_, err := MakeURN(prefixWithTwoSlashes)
		require.Error(t, err)
	})
}

func TestSkolemize(t *testing.T) {

	t.Run("empty nq does nothing", func(t *testing.T) {
		// TODO check do we want an empty nq to error?
		output, err := Skolemization("")
		require.NoError(t, err)
		require.Equal(t, "", output)
	})

	t.Run("full nq with no replacements", func(t *testing.T) {
		const nq = "<https://urn.io/xid/genid/1> <https://urn.io/xid/genid/2> <https://urn.io/xid/genid/3> ."
		output, err := Skolemization(nq)
		require.NoError(t, err)
		require.Equal(t, nq, output)
	})

	t.Run("full nq with one replacement", func(t *testing.T) {
		const emptyNode = "_:"
		const nonEmptyNodes = "<https://urn.io/xid/genid/2> <https://urn.io/xid/genid/3> ."
		nq := emptyNode + " " + nonEmptyNodes

		output, err := Skolemization(nq)
		require.NoError(t, err)
		require.Contains(t, output, nonEmptyNodes)
		require.NotContains(t, output, emptyNode)
		require.Contains(t, output, "<https://iow.io/xid/genid/")
	})

}
