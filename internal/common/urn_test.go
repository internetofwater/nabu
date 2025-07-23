// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeUrn(t *testing.T) {
	t.Run("prefix with no slashes fails", func(t *testing.T) {
		const shortPrefixWithNoSlashes = "test"
		_, err := MakeURN(shortPrefixWithNoSlashes)
		require.Error(t, err)
	})

	t.Run("prefix with 3 parts preserves all three and doesnt change order", func(t *testing.T) {
		result, err := MakeURN("test1/test2/test3")
		require.NoError(t, err)
		require.Equal(t, "urn:iow:test1:test2:test3", result)
	})

	t.Run("prefix with 4 parts preserves all four", func(t *testing.T) {
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
		require.Empty(t, output)
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

		hash := sha256.New()
		split := strings.Split(output, " ")
		hash.Write([]byte(split[1]))
		hash.Write([]byte(split[2]))
		hashResult := hex.EncodeToString(hash.Sum(nil))
		require.Equal(t, hashResult, "0adc62bdb95a47b9d52d8dff5e78957b1da6448e7d43fad18a4d8f9b1ccc032c")
		require.Contains(t, output, hashResult)
	})

}
