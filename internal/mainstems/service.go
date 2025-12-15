// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"encoding/json"

	"github.com/internetofwater/nabu/internal/common"
)

// A response from a mainstem service
type MainstemQueryResponse struct {
	// whether or not the service found an associated mainstem
	// some databases may not contain mainstems due to the mainstem
	// being too small and the dataset not containing small mainstems
	foundAssociatedMainstem bool
	// the uri to mainstem itself; i.e. https://geoconnex.us/ref/mainstems/1
	mainstemURI string
}

// A mainstem service resolves geometry to the associated mainstem
type MainstemService interface {
	// Given a wkt geometry return the uri of the associated mainstem
	GetMainstemForWkt(wkt string) (MainstemQueryResponse, error)
}

// A jsonld enricher adds extra information to jsonld
// such as the associated mainstem
type JsonldEnricher struct {
	service MainstemService
}

func NewJsonldEnricher(service MainstemService) *JsonldEnricher {
	return &JsonldEnricher{
		service: service,
	}
}

// Given a jsonld, add mainstem information to it
func (j *JsonldEnricher) AddMainstemInfo(jsonld []byte) (newJsonld []byte, err error) {
	var serializedJson map[string]any
	err = json.Unmarshal(jsonld, &serializedJson)
	if err != nil {
		return nil, err
	}

	wkt, ok := common.GetWktFromJsonld(serializedJson)
	if !ok {
		// if there is no geometry, there is no way to attach a mainstem
		// and thus we can just return the original jsonld without any error
		// since some jsonld may not have a geometry (i.e. from provenance data)
		return jsonld, nil
	}

	newJsonldAsMap, err := common.AddKeyToJsonLDContext(serializedJson,
		"hyf", "https://www.opengis.net/def/appschema/hy_features/hyf/")
	if err != nil {
		return nil, err
	}

	_, err = j.service.GetMainstemForWkt(wkt)
	if err != nil {
		return nil, err
	}

	return json.Marshal(newJsonldAsMap)
}
