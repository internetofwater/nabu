package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkolemize(t *testing.T) {

	t.Run("empty nq does nothing", func(t *testing.T) {
		// TODO check do we want an empty nq to error?
		output, err := Skolemization("")
		require.NoError(t, err)
		require.Equal(t, "", output)
	})

	t.Run("full nq with no replacements", func(t *testing.T) {
		const nq = "<https://gleaner.io/xid/genid/1> <https://gleaner.io/xid/genid/2> <https://gleaner.io/xid/genid/3> ."
		output, err := Skolemization(nq)
		require.NoError(t, err)
		require.Equal(t, nq, output)
	})

	t.Run("full nq with one replacement", func(t *testing.T) {
		const nq2 = "_: <https://gleaner.io/xid/genid/2> <https://gleaner.io/xid/genid/3> ."
		output, err := Skolemization(nq2)
		require.NoError(t, err)
		expectedResp := "<https://gleaner.io/xid/genid/cul06536f3jg4qao2870> <https://gleaner.io/xid/genid/2> <https://gleaner.io/xid/genid/3> ."
		require.NotEqual(t, expectedResp, output)
	})

}
