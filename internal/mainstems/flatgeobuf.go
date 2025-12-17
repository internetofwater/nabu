// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"bytes"
	"fmt"
	"os"

	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/packedrtree"
	geom "github.com/peterstace/simplefeatures/geom"
)

type S3FlatgeobufMainstemService struct {
	// the flatgeobuf URI must be a local
	// path; remote is not yet supported
	mainstemFlatgeobufURI string
}

var _ MainstemService = S3FlatgeobufMainstemService{}

func NewS3FlatgeobufMainstemService(mainstemFlatgeobufURI string) (S3FlatgeobufMainstemService, error) {
	return S3FlatgeobufMainstemService{mainstemFlatgeobufURI: mainstemFlatgeobufURI}, nil
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

	bbox := packedrtree.Box{
		XMin: coordinates.X,
		YMin: coordinates.Y,
		XMax: coordinates.X,
		YMax: coordinates.Y,
	}

	file, err := os.Open(s.mainstemFlatgeobufURI)
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when opening flatgeobuf %v", err)
	}

	fileReader := flatgeobuf.NewFileReader(file)

	_, err = fileReader.Header()
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when reading header from flatgeobuf %v", err)
	}

	features, err := fileReader.IndexSearch(bbox)
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when running index search on flatgeobuf %v", err)
	}

	if len(features) == 0 {
		return MainstemQueryResponse{mainstemURI: "", foundAssociatedMainstem: false}, nil
	}

	if len(features) > 1 {
		return MainstemQueryResponse{}, fmt.Errorf("got more than one mainstem result for WKT: %s", wkt)
	}

	propsBuf := features[0].PropertiesBytes()
	propsReader := flatgeobuf.NewPropReader(bytes.NewReader(propsBuf))

	propsVals, err := propsReader.ReadSchema(&features[0])

	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when reading properties from flatgeobuf %v", err)
	}

	result := flatgeobuf.FeatureString(&features[0], &features[0])
	fmt.Print(result)

	for _, prop := range propsVals {
		if prop.String() == "geoconnex_url" {
			return MainstemQueryResponse{mainstemURI: prop.Value.(string), foundAssociatedMainstem: true}, nil
		}
	}

	// property not present
	return MainstemQueryResponse{mainstemURI: "", foundAssociatedMainstem: false}, nil
}
