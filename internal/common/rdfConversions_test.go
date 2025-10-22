// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertBetweenQuadsAndTriples(t *testing.T) {
	const nq = "<https://nabu.io/id/org/ref_gages_gages__0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/Organization> <urn:iow:orgs:ref_gages_gages__0.nq> ."
	graph, err := QuadsToTripleWithCtx(nq)
	require.NoError(t, err)
	require.NotEmpty(t, graph.GraphURI)
	require.NotEmpty(t, graph.Triples)

	nq2, err := NtToNq(graph.Triples, graph.GraphURI)
	require.NoError(t, err)
	require.Equal(t, nq, strings.TrimSuffix(nq2, "\n"))
}
