// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	geom "github.com/peterstace/simplefeatures/geom"
	log "github.com/sirupsen/logrus"
)

type S3FlatgeobufMainstemService struct {
	duckdb *sql.DB

	mainstemFlatgeobufURI string
}

var _ MainstemService = S3FlatgeobufMainstemService{}

// Create a mainstem service that loads the catchments in the flatgeobuf file into an in memory table.
// Querying the flatgeobuf directly reopens the file and rereads its spatial index for every feature,
// which is especially slow when the file is remote, so it is read once here instead
func NewS3FlatgeobufMainstemService(mainstemFlatgeobufURI string) (S3FlatgeobufMainstemService, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return S3FlatgeobufMainstemService{}, err
	}
	if err := loadMainstems(db, mainstemFlatgeobufURI); err != nil {
		_ = db.Close()
		return S3FlatgeobufMainstemService{}, err
	}
	return S3FlatgeobufMainstemService{duckdb: db, mainstemFlatgeobufURI: mainstemFlatgeobufURI}, nil
}

// Copy the catchments with a mainstem into an in memory table with a spatial index
func loadMainstems(db *sql.DB, mainstemFlatgeobufURI string) error {
	start := time.Now()
	if _, err := db.Exec("INSTALL spatial; LOAD spatial;"); err != nil {
		return err
	}
	// only the columns needed for the lookup are kept so the table is a fraction of the size
	// of the file; catchments without a mainstem would never produce an association
	if _, err := db.Exec(`
		CREATE TABLE mainstems AS
			SELECT geoconnex_url, geom
			FROM ST_Read(?)
			WHERE geoconnex_url IS NOT NULL AND geoconnex_url != ''
	`, mainstemFlatgeobufURI); err != nil {
		return fmt.Errorf("failed to load mainstems from %s: %w", mainstemFlatgeobufURI, err)
	}
	if _, err := db.Exec("CREATE INDEX mainstems_geom_idx ON mainstems USING RTREE (geom)"); err != nil {
		return fmt.Errorf("failed to index mainstems from %s: %w", mainstemFlatgeobufURI, err)
	}
	var rows int64
	if err := db.QueryRow("SELECT count(*) FROM mainstems").Scan(&rows); err != nil {
		return err
	}
	log.Infof("Loaded %d catchments with mainstems from %s into memory in %s", rows, mainstemFlatgeobufURI, time.Since(start))
	return nil
}

func (s S3FlatgeobufMainstemService) GetMainstemForWkt(ctx context.Context, wkt string) (MainstemQueryResponse, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, "GetMainstemForWkt")
	defer span.End()

	geometry, err := geom.UnmarshalWKT(wkt)
	if err != nil {
		return MainstemQueryResponse{}, &InvalidWktError{message: fmt.Sprintf("failed to parse WKT: %v", err)}
	}
	point := geometry.Centroid()
	coordinates, isNonEmpty := point.Coordinates()
	if !isNonEmpty {
		return MainstemQueryResponse{}, fmt.Errorf("got an empty centroid result for WKT: %s", wkt)
	}

	// the rtree index narrows the search to the catchments whose bounding box contains
	// the centroid and ST_Intersects then picks the catchment that actually contains it;
	// catchments do not overlap so there is at most one match
	const mainstemSQL = `
		SELECT geoconnex_url
		FROM mainstems
		WHERE ST_Intersects(geom, ST_Point(?, ?))
		LIMIT 1
	`
	var mainstemURI string
	err = s.duckdb.QueryRowContext(ctx, mainstemSQL, coordinates.X, coordinates.Y).Scan(&mainstemURI)
	if errors.Is(err, sql.ErrNoRows) {
		return MainstemQueryResponse{foundAssociatedMainstem: false, mainstemURI: ""}, nil
	} else if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("mainstem query failed for %s: %w", wkt, err)
	}
	return MainstemQueryResponse{
		foundAssociatedMainstem: true,
		mainstemURI:             mainstemURI,
	}, nil
}
