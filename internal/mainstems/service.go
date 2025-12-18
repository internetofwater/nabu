// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"bytes"
	"encoding/json"
	"errors"
	"html/template"

	"github.com/internetofwater/nabu/internal/common"
	log "github.com/sirupsen/logrus"
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
func (j *JsonldEnricher) AddMainstemInfo(jsonld []byte) (newJsonld []byte, addedMainstem bool, err error) {
	var serializedJson map[string]any
	err = json.Unmarshal(jsonld, &serializedJson)
	if err != nil {
		return nil, false, err
	}

	wkt, ok := common.GetWktFromJsonld(serializedJson)
	if !ok {
		// if there is no geometry, there is no way to attach a mainstem
		// and thus we can just return the original jsonld without any error
		// since some jsonld may not have a geometry (i.e. from provenance data)
		log.Warn("no geometry found in jsonld; skipping adding mainstem info")
		return jsonld, false, nil
	}

	newJsonldAsMap, err := common.AddKeyToJsonLDContext(serializedJson,
		"hyf", "https://www.opengis.net/def/appschema/hy_features/hyf/")
	if err != nil {
		return nil, false, err
	}

	mainstemResponse, err := j.service.GetMainstemForWkt(wkt)
	if err != nil {
		return nil, false, err
	}

	if !mainstemResponse.foundAssociatedMainstem {
		log.Debugf("no mainstem found for %s", wkt)
		newJson, err := json.Marshal(newJsonldAsMap)
		return newJson, false, err
	}

	newJsonldAsMap, err = AddMainstemToJsonLD(newJsonldAsMap, mainstemResponse.mainstemURI)
	if err != nil {
		return nil, false, err
	}
	jsonld, err = json.Marshal(newJsonldAsMap)
	if err != nil {
		return nil, false, err
	}
	return jsonld, true, err
}

func AddMainstemToJsonLD(jsonldMap map[string]any, mainstemURI string) (map[string]any, error) {
	if mainstemURI == "" {
		return nil, errors.New("mainstem URI is empty")
	}

	if _, ok := jsonldMap["hyf:referencedPosition"]; ok {
		// Mainstem already present
		return jsonldMap, nil
	}

	jsonldMap, err := common.AddKeyToJsonLDContext(jsonldMap,
		"hyf", "https://www.opengis.net/def/schema/hy_features/hyf/",
	)
	if err != nil {
		return nil, err
	}

	// Template with mainstem URI placeholder
	const referencedPositionTemplate = `
	{
		"hyf:referencedPosition": [
			{
				"hyf:HY_IndirectPosition": {
					"hyf:distanceDescription": {
						"hyf:HY_DistanceDescription": "upstream"
					},
					"hyf:linearElement": {"@id": "{{.MainstemURI}}"}
				}
			}
		]
	}`

	tmpl, err := template.New("referencedPosition").Parse(referencedPositionTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]string{
		"MainstemURI": mainstemURI,
	})
	if err != nil {
		return nil, err
	}

	var referencedPosition any
	err = json.Unmarshal(buf.Bytes(), &referencedPosition)
	if err != nil {
		return nil, err
	}

	jsonldMap["hyf:referencedPosition"] = referencedPosition.(map[string]any)["hyf:referencedPosition"]
	return jsonldMap, nil
}
