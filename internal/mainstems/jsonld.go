// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	hyfNamespace = "https://www.opengis.net/def/schema/hy_features/hyf/"
	// properties are written as full IRIs so that the document's
	// @context does not need to be changed to add them
	referencedPositionIRI = hyfNamespace + "referencedPosition"
)

// ErrCannotAddMainstem is returned when the shape of a JSON-LD document
// does not have a single top level node that a mainstem can be added to
var ErrCannotAddMainstem = errors.New("JSON-LD document does not have a single top level node to add a mainstem to")

// AddMainstemToJsonld returns the JSON-LD document with a hyf:referencedPosition that links it
// to the upstream mainstem. Documents that already reference a position are returned as is
func AddMainstemToJsonld(jsonld []byte, mainstemURI string) ([]byte, error) {
	if mainstemURI == "" {
		return nil, errors.New("mainstem URI is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonld))
	// keep numbers as they were written instead of converting them to float64
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return nil, fmt.Errorf("%w: %v", ErrCannotAddMainstem, err)
		}
		return nil, err
	}
	if _, isGraph := doc["@graph"]; isGraph && doc["@id"] == nil {
		return nil, fmt.Errorf("%w: documents with a @graph are not supported", ErrCannotAddMainstem)
	}

	for _, existing := range []string{"hyf:referencedPosition", referencedPositionIRI} {
		if _, ok := doc[existing]; ok {
			return jsonld, nil
		}
	}

	doc[referencedPositionIRI] = []any{
		map[string]any{
			hyfNamespace + "HY_IndirectPosition": map[string]any{
				hyfNamespace + "distanceDescription": map[string]any{
					hyfNamespace + "HY_DistanceDescription": "upstream",
				},
				hyfNamespace + "linearElement": map[string]any{"@id": mainstemURI},
			},
		},
	}

	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	// urls commonly contain & which should be left as is
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(doc); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
}
