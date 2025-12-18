// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/gogama/flatgeobuf/flatgeobuf"
	"github.com/gogama/flatgeobuf/flatgeobuf/flat"
	"github.com/gogama/flatgeobuf/packedrtree"
	geom "github.com/peterstace/simplefeatures/geom"
	log "github.com/sirupsen/logrus"
)

type S3FlatgeobufMainstemService struct {
	mainstemFlatgeobufURI string
}

var _ MainstemService = S3FlatgeobufMainstemService{}

func NewS3FlatgeobufMainstemService(mainstemFlatgeobufURI string) (S3FlatgeobufMainstemService, error) {
	return S3FlatgeobufMainstemService{mainstemFlatgeobufURI: mainstemFlatgeobufURI}, nil
}

// Custom property reader that handles nullable/missing properties correctly
func readPropertiesManually(propsBuf []byte, header *flat.Header) (map[string]any, error) {
	properties := make(map[string]interface{})
	reader := bytes.NewReader(propsBuf)

	// Build a map of column index to column info for quick lookup
	columnMap := make(map[uint16]*flat.Column)
	for i := 0; i < header.ColumnsLength(); i++ {
		col := new(flat.Column)
		if header.Columns(col, i) {
			columnMap[uint16(i)] = col
		}
	}

	// Read properties until we run out of data
	for {
		// Read column index (2 bytes, little-endian)
		var colIndex uint16
		err := binary.Read(reader, binary.LittleEndian, &colIndex)
		if err == io.EOF {
			// End of properties buffer
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read column index: %v", err)
		}

		// Get the column info
		col, exists := columnMap[colIndex]
		if !exists {
			return nil, fmt.Errorf("invalid column index: %d", colIndex)
		}

		// Read the value based on column type
		var value interface{}
		switch col.Type() {
		case flat.ColumnTypeByte:
			var v int8
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeUByte:
			var v uint8
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeBool:
			var v uint8
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v != 0
		case flat.ColumnTypeShort:
			var v int16
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeUShort:
			var v uint16
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeInt:
			var v int32
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeUInt:
			var v uint32
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeLong:
			var v int64
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeULong:
			var v uint64
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeFloat:
			var v float32
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeDouble:
			var v float64
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeString, flat.ColumnTypeJson:
			// String: 4 bytes length + N bytes data
			var length uint32
			err = binary.Read(reader, binary.LittleEndian, &length)
			if err != nil {
				return nil, fmt.Errorf("failed to read string length for column %s: %v", string(col.Name()), err)
			}
			strBytes := make([]byte, length)
			_, err = reader.Read(strBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to read string data for column %s: %v", string(col.Name()), err)
			}
			value = string(strBytes)
		case flat.ColumnTypeDateTime:
			var v uint64
			err = binary.Read(reader, binary.LittleEndian, &v)
			value = v
		case flat.ColumnTypeBinary:
			// Binary: 4 bytes length + N bytes data
			var length uint32
			err = binary.Read(reader, binary.LittleEndian, &length)
			if err != nil {
				return nil, fmt.Errorf("failed to read binary length for column %s: %v", string(col.Name()), err)
			}
			binaryBytes := make([]byte, length)
			_, err = reader.Read(binaryBytes)
			value = binaryBytes
		default:
			return nil, fmt.Errorf("unsupported column type: %v for column %s", col.Type(), string(col.Name()))
		}

		if err != nil {
			return nil, fmt.Errorf("failed to read value for column %s: %v", string(col.Name()), err)
		}

		properties[string(col.Name())] = value
	}

	return properties, nil
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
	defer func() {
		err = file.Close()
		if err != nil {
			log.Error(err)
		}
	}()

	filereader := flatgeobuf.NewFileReader(file)
	header, err := filereader.Header()
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when reading flatgeobuf header %v", err)
	}

	err = filereader.Rewind()
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when rewinding flatgeobuf %v", err)
	}

	features, err := filereader.IndexSearch(bbox)
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when running index search on flatgeobuf %v", err)
	}

	if len(features) == 0 {
		return MainstemQueryResponse{mainstemURI: "", foundAssociatedMainstem: false}, nil
	}

	if len(features) > 1 {
		return MainstemQueryResponse{}, fmt.Errorf("got more than one mainstem result for WKT: %s", wkt)
	}

	// Use custom property reader that handles nullable properties correctly
	propsBuf := features[0].PropertiesBytes()
	properties, err := readPropertiesManually(propsBuf, header)
	if err != nil {
		return MainstemQueryResponse{}, fmt.Errorf("error when reading properties from flatgeobuf %v", err)
	}

	// Look for geoconnex_url property
	if uri, exists := properties["geoconnex_url"]; exists && uri != nil {
		if uriStr, ok := uri.(string); ok && uriStr != "" {
			return MainstemQueryResponse{mainstemURI: uriStr, foundAssociatedMainstem: true}, nil
		}
	}

	return MainstemQueryResponse{mainstemURI: "", foundAssociatedMainstem: false}, nil
}
