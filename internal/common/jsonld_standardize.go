// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// given a jsonld context, this function will standardize the prefix of the context
// to ensure minor changes like http vs https or www vs non-www do not cause the context to be considered different
func StandardizeJsonldContextWithMutation(jsonld map[string]any) (mutatedJsonld map[string]any, err error) {
	if jsonld == nil {
		return nil, errors.New("nil jsonld input when trying to standardize context")
	}

	ctxRaw, ok := jsonld["@context"]
	if !ok {
		jsonld_as_string, err := json.Marshal(jsonld)
		if err != nil {
			return nil, fmt.Errorf("tried to convert jsonld to string for the purposes of creating error message but failed: %w", err)
		}
		return jsonld, fmt.Errorf("tried to standardize context for jsonld %s with no @context to standardize", string(jsonld_as_string))
	}

	standardized, err := standardizeContext(ctxRaw)
	if err != nil {
		return nil, err
	}

	jsonld["@context"] = standardized
	return jsonld, nil
}

func standardizeContext(jsonldContext any) (any, error) {
	switch c := jsonldContext.(type) {

	case string:
		// if the context is just a string we want to standardize it directly
		return standardizeIRI(c), nil

	case []any:
		// if the context is an array of values without any mapping,
		// we can recursively standardize the subset
		for i, item := range c {
			normalized, err := standardizeContext(item)
			if err != nil {
				return nil, err
			}
			c[i] = normalized
		}
		return c, nil
	case map[string]any:
		// if the context is a map, we want to standardize the value of
		// each key in the map
		for k, v := range c {

			strVal, ok := v.(string)
			if !ok {
				continue
			}

			c[k] = standardizeIRI(strVal)
		}
		return c, nil

	default:
		// TODO maybe make this an error? unclear
		// if arbitrary other contexts should be allowed or not
		return jsonldContext, nil
	}
}

func standardizeIRI(iri string) string {

	if strings.Contains(iri, "http://schema.org") {
		return "https://schema.org/"
	}

	if strings.Contains(iri, "http://www.opengeospatial.org/standards/waterml2/hy_features") {
		return "https://www.opengis.net/def/schema/hy_features/hyf/"
	}

	if strings.Contains(iri, "https://www.opengis.net/def/appschema/hy_features/hyf") {
		return "https://www.opengis.net/def/schema/hy_features/hyf/"
	}

	return iri
}
