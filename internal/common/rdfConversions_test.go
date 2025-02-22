package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertBetweenQuadsAndTriples(t *testing.T) {
	const nq = "<https://gleaner.io/id/org/ref_gages_gages__0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Organization> <urn:iow:orgs:ref_gages_gages__0.nq> ."
	triples, ctx, err := QuadsToTripleWithCtx(nq)
	require.NoError(t, err)
	require.NotEmpty(t, triples)
	require.NotEmpty(t, ctx)

	nq2, err := NtToNq(triples, ctx)
	require.NoError(t, err)
	require.Equal(t, nq, strings.TrimSuffix(nq2, "\n"))
}
