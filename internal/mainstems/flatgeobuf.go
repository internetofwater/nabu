// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

type S3FlatgeobufMainstemService struct {
	duckdb *sql.DB
	// the flatgeobuf URI can be either
	// a local path like ./data.fgb or
	// a remote object storage like
	// gcs://national-hydrologic-geospatial-fabric-reference-hydrofabric/reference_catchments_and_flowlines.fgb
	mainstemFlatgeobufURI string
}

var _ MainstemService = S3FlatgeobufMainstemService{}

func NewS3FlatgeobufMainstemService(mainstemFlatgeobufURI string) (S3FlatgeobufMainstemService, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return S3FlatgeobufMainstemService{}, err
	}
	_, err = db.Exec("INSTALL spatial; LOAD spatial;")
	if err != nil {
		return S3FlatgeobufMainstemService{}, err
	}
	return S3FlatgeobufMainstemService{duckdb: db, mainstemFlatgeobufURI: mainstemFlatgeobufURI}, nil
}

func (s S3FlatgeobufMainstemService) Close() error {
	return s.duckdb.Close()
}
func (s S3FlatgeobufMainstemService) GetMainstemForWkt(wkt string) (MainstemQueryResponse, error) {
	// We first query the centroid of the geometry
	// so that we are guaranteed to only get one
	// catchment and thus only one maistem; otherwise
	// there could be multiple overlapping and thus
	// ambiguity
	centroidQuery := `
    SELECT
        ST_X(ST_Centroid(g)) AS center_x,
        ST_Y(ST_Centroid(g)) AS center_y
    FROM (
        SELECT ST_GeomFromText(CAST(? AS VARCHAR)) AS g
    )
	`
	row := s.duckdb.QueryRow(centroidQuery, wkt)
	if row.Err() != nil {
		return MainstemQueryResponse{}, row.Err()
	}
	var center_x, center_y float64
	if err := row.Scan(&center_x, &center_y); err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("centroid query failed: %w", err)
	}

	// flatgeobuf requires opening with a bbox in duckdb
	// in order to subset the data; by using the same
	// value for min and max we get a specific point
	// and a guarantee of no overlaps
	mainstemSQL := `
    SELECT geoconnex_url
		FROM ST_Read(
			?,
			spatial_filter_box = ST_MakeBox2D(
				ST_Point(?, ?),
				ST_Point(?, ?)
			)
		)
	`
	result := s.duckdb.QueryRow(mainstemSQL, s.mainstemFlatgeobufURI, center_x, center_y, center_x, center_y)
	if result.Err() != nil {
		return MainstemQueryResponse{}, fmt.Errorf("mainstem query failed: %w", result.Err())
	}
	var mainstemURI sql.NullString
	if err := result.Scan(&mainstemURI); err != nil {
		return MainstemQueryResponse{}, err
	}
	if mainstemURI.Valid && mainstemURI.String != "" {
		return MainstemQueryResponse{
			foundAssociatedMainstem: true,
			mainstemURI:             mainstemURI.String,
		}, nil
	}
	return MainstemQueryResponse{
		foundAssociatedMainstem: false,
		mainstemURI:             "",
	}, nil
}
