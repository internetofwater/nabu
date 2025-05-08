// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

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

func TestMakeReleaseName(t *testing.T) {

	res, err := makeReleaseNqName("summoned/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_release.nq", res)

	res, err = makeReleaseNqName("prov/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_prov.nq", res)

	res, err = makeReleaseNqName("orgs/counties0")
	require.NoError(t, err)
	require.Equal(t, "counties0_organizations.nq", res)
	res, err = makeReleaseNqName("orgs/")
	require.NoError(t, err)
	require.Equal(t, "organizations.nq", res)

	_, err = makeReleaseNqName("orgs")
	require.Error(t, err)
}

func TestAllocateBatches(t *testing.T) {
	tests := []struct {
		name       string
		graphNames []string
		batchSize  int
		want       [][]string
	}{
		{
			name:       "even split",
			graphNames: []string{"a", "b", "c", "d"},
			batchSize:  2,
			want:       [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:       "uneven split",
			graphNames: []string{"a", "b", "c", "d", "e"},
			batchSize:  2,
			want:       [][]string{{"a", "b"}, {"c", "d"}, {"e"}},
		},
		{
			name:       "batch size greater than input",
			graphNames: []string{"a", "b"},
			batchSize:  5,
			want:       [][]string{{"a", "b"}},
		},
		{
			name:       "batch size is 1",
			graphNames: []string{"a", "b", "c"},
			batchSize:  1,
			want:       [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			name:       "empty input",
			graphNames: []string{},
			batchSize:  3,
			want:       [][]string{}, // Ensure this matches the return value: [][]string{}
		},
		{
			name:       "batch size is 0",
			graphNames: []string{"a", "b"},
			batchSize:  0,
			want:       [][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := allocateBatches(tt.graphNames, tt.batchSize)
			require.Equal(t, tt.want, got)
		})
	}
}
