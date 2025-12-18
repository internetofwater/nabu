// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPointInFlatgeobuf(t *testing.T) {
	const fgb = "./testdata/boston_catchments.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)

	response, err := service.GetMainstemForWkt("POINT(-71.0839 42.3477)")
	require.NoError(t, err)
	require.Equal(t, "https://reference.geoconnex.us/collections/mainstems/items/2290857", response.mainstemURI)

	response, err = service.GetMainstemForWkt("POINT(-180 -170)")
	require.NoError(t, err)
	require.Empty(t, response.mainstemURI)
	require.False(t, response.foundAssociatedMainstem)

}

func TestPointThatCausedMultipleResults(t *testing.T) {
	const fgb = "./testdata/colorado_subset.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)

	response, err := service.GetMainstemForWkt("POINT (-108.00852774278917 37.2266879422167)")
	require.NoError(t, err)
	require.Empty(t, response.mainstemURI)
	require.False(t, response.foundAssociatedMainstem)

}
