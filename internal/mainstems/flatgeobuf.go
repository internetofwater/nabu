// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	geom "github.com/peterstace/simplefeatures/geom"
	log "github.com/sirupsen/logrus"
)

type S3FlatgeobufMainstemService struct {
	duckdb *sql.DB

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

func (s S3FlatgeobufMainstemService) GetMainstemForWkt(wkt string) (MainstemQueryResponse, error) {
	geometry, err := geom.UnmarshalWKT(wkt)
	if err != nil {
		return MainstemQueryResponse{}, err
	}
	point := geometry.Centroid()
	coordinates, isNonEmpty := point.Coordinates()
	if !isNonEmpty {
		return MainstemQueryResponse{}, fmt.Errorf("got an empty centroid result for WKT: %s", wkt)
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
	result := s.duckdb.QueryRow(mainstemSQL, s.mainstemFlatgeobufURI, coordinates.X, coordinates.Y, coordinates.X, coordinates.Y)
	if result.Err() != nil {
		return MainstemQueryResponse{}, fmt.Errorf("mainstem query failed for %s: %w", wkt, result.Err())
	}
	var mainstemURI sql.NullString
	if err := result.Scan(&mainstemURI); err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("failed to get sql result for query at %s, %v", wkt, err)
	}
	if mainstemURI.Valid && mainstemURI.String != "" {
		return MainstemQueryResponse{
			foundAssociatedMainstem: true,
			mainstemURI:             mainstemURI.String,
		}, nil
	}
	log.Warnf("no mainstem found for %s: %s", wkt, mainstemURI.String)
	return MainstemQueryResponse{
		foundAssociatedMainstem: false,
		mainstemURI:             "",
	}, nil
}
