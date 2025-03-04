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
