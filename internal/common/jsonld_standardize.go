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

func standardizeContext(ctx any) (any, error) {
	switch c := ctx.(type) {

	case string:
		return standardizeIRI(c)

	case []any:
		for i, item := range c {
			normalized, err := standardizeContext(item)
			if err != nil {
				return nil, err
			}
			c[i] = normalized
		}
		return c, nil

	case map[string]any:
		for k, v := range c {
			// Skip JSON-LD keywords as keys
			if strings.HasPrefix(k, "@") {
				continue
			}

			strVal, ok := v.(string)
			if !ok {
				continue
			}

			normalized, err := standardizeIRI(strVal)
			if err != nil {
				return nil, err
			}
			c[k] = normalized
		}
		return c, nil

	default:
		return ctx, nil
	}
}

func standardizeIRI(iri string) (string, error) {
	if strings.HasPrefix(iri, "http://") {
		iri = "https://" + strings.TrimPrefix(iri, "http://")
		return iri, nil
	}

	return iri, nil
}
