// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"context"
	"fmt"
	"testing"

	"github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestPointInFlatgeobuf(t *testing.T) {
	const fgb = "./testdata/boston_catchments.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)

	response, err := service.GetMainstemForWkt(context.Background(), "POINT(-71.0839 42.3477)")
	require.NoError(t, err)
	require.Equal(t, "https://reference.geoconnex.us/collections/mainstems/items/2290857", response.mainstemURI)

	response, err = service.GetMainstemForWkt(context.Background(), "POINT(-180 -170)")
	require.NoError(t, err)
	require.Empty(t, response.mainstemURI)
	require.False(t, response.foundAssociatedMainstem)

}

func TestEdgeCases(t *testing.T) {
	const fgb = "./testdata/colorado_subset.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)

	const insideDatasetButNullValue = "POINT(-108.00852774278917 37.2266879422167)"
	response, err := service.GetMainstemForWkt(context.Background(), insideDatasetButNullValue)
	require.NoError(t, err)
	require.Empty(t, response.mainstemURI)
	require.False(t, response.foundAssociatedMainstem)

	const regionOutsideOfDataset = "POINT(-107.74580000048866 36.958399999924836)"
	response, err = service.GetMainstemForWkt(context.Background(), regionOutsideOfDataset)
	require.NoError(t, err)
	require.Empty(t, response.mainstemURI)
	require.False(t, response.foundAssociatedMainstem)
}

func TestInvalidWkt(t *testing.T) {

	const fgb = "./testdata/colorado_subset.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)
	const wktWithOverlappingVertices = "POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))"

	_, err = service.GetMainstemForWkt(context.Background(), wktWithOverlappingVertices)
	var invalidWktErr *InvalidWktError
	require.ErrorAs(t, err, &invalidWktErr)
}

// Ensure the service can be queried concurrently without a mutex;
// older versions of duckdb spatial had a race condition in GDAL file system
// registration that caused "file does not exist" errors
// https://github.com/duckdb/duckdb-spatial/issues/728
func TestConcurrentQueries(t *testing.T) {
	const fgb = "./testdata/boston_catchments.fgb"

	service, err := NewS3FlatgeobufMainstemService(fgb)
	require.NoError(t, err)

	var group errgroup.Group
	group.SetLimit(20)
	for range 500 {
		group.Go(func() error {
			response, err := service.GetMainstemForWkt(context.Background(), "POINT(-71.0839 42.3477)")
			if err != nil {
				return err
			}
			if response.mainstemURI != "https://reference.geoconnex.us/collections/mainstems/items/2290857" {
				return fmt.Errorf("unexpected mainstem uri %q", response.mainstemURI)
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
}

func TestGetMainstemURIForWkb(t *testing.T) {
	service, err := NewS3FlatgeobufMainstemService("./testdata/boston_catchments.fgb")
	require.NoError(t, err)

	point, err := geom.UnmarshalWKT("POINT(-71.0839 42.3477)")
	require.NoError(t, err)
	uri, err := GetMainstemURIForWkb(context.Background(), service, point.AsBinary())
	require.NoError(t, err)
	require.Equal(t, "https://reference.geoconnex.us/collections/mainstems/items/2290857", uri)

	uri, err = GetMainstemURIForWkb(context.Background(), service, nil)
	require.NoError(t, err)
	require.Empty(t, uri, "features without a geometry have no mainstem")

	invalid, err := geom.UnmarshalWKT("POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))", geom.NoValidate{})
	require.NoError(t, err)
	uri, err = GetMainstemURIForWkb(context.Background(), service, invalid.AsBinary())
	require.NoError(t, err, "invalid geometries should not be a fatal error")
	require.Empty(t, uri)
}
