// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"

	"github.com/internetofwater/nabu/internal/common"
	geom "github.com/peterstace/simplefeatures/geom"
	log "github.com/sirupsen/logrus"
)

// A service that returns the mainstem for a given point
// by using live USGS NLDI APIs
type USGSMainstemService struct {
	httpClient *http.Client
}

var _ MainstemService = USGSMainstemService{}

func NewUSGSMainstemService(httpClient *http.Client) USGSMainstemService {
	return USGSMainstemService{
		httpClient: httpClient,
	}
}

type featureResponse struct {
	Type_    string `json:"type"`
	Geometry struct {
		Coordinates [][][]float64 `json:"coordinates"`
	} `json:"geometry"`
	Properties struct {
		Featureid int64 `json:"featureid"`
	} `json:"properties"`
}

type catchmentResponse struct {
	Type_    string            `json:"type"`
	Features []featureResponse `json:"features"`
}

func (r USGSMainstemService) getAssociatedCatchment(longitude float64, latitude float64) (featureID int64, err error) {
	const catchmentspServiceURL = "https://labs-beta.waterdata.usgs.gov/api/fabric/pygeoapi/collections/catchmentsp/items?f=json&skipGeometry=true"
	urlWithBbox := fmt.Sprintf("%s&bbox=%f,%f,%f,%f", catchmentspServiceURL, longitude, latitude, longitude, latitude)

	resp, err := r.httpClient.Get(urlWithBbox)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("failed to get catchment for point: %s", resp.Status)
	}
	var catchmentResponse catchmentResponse
	err = json.NewDecoder(resp.Body).Decode(&catchmentResponse)
	if err != nil {
		return 0, err
	}
	if catchmentResponse.Type_ != "FeatureCollection" {
		return 0, fmt.Errorf("expected FeatureCollection with one item for %s,but got unexpected response type: %s", urlWithBbox, catchmentResponse.Type_)
	}

	return catchmentResponse.Features[0].Properties.Featureid, nil
}

type flowLineFeature struct {
	Type_      string `json:"type"`
	Properties struct {
		Nhdplus_comid string `json:"nhdplus_comid"`
	}
}

type flowlineResponse struct {
	Type_    string            `json:"type"`
	Features []flowLineFeature `json:"features"`
}

func (r USGSMainstemService) getUpstreamFeatureOfCatchment(featureId int64) (string, error) {
	url := fmt.Sprintf("https://api.water.usgs.gov/nldi/linked-data/comid/%d/navigation/UM/flowlines?f=json&distance=3000", featureId)

	resp, err := r.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to get flowline for feature: %s", resp.Status)
	}
	var flowlineResponse flowlineResponse
	err = json.NewDecoder(resp.Body).Decode(&flowlineResponse)
	if err != nil {
		return "", err
	}
	// the last feature is the most upstream feature according to the nhdpv2 dataset, aka the headwater -> head -> head_nhdpv2_comid
	mostUpstreamFeature := len(flowlineResponse.Features) - 1
	return flowlineResponse.Features[mostUpstreamFeature].Properties.Nhdplus_comid, nil
}

func (r USGSMainstemService) getGeoconnexURIFromComid(comid string) (geoconnexURI string, err error) {
	comidURI := "https://geoconnex.us/nhdplusv2/comid/" + comid
	url := "https://reference.geoconnex.us/collections/mainstems/items?f=json&skipGeometry=true&head_nhdpv2_comid=" + comidURI
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to get mainstem for comid: %s", resp.Status)
	}
	var mainstemResponse struct {
		Type_    string `json:"type"`
		Features []struct {
			Properties struct {
				URI string `json:"uri"`
			} `json:"properties"`
		} `json:"features"`
	}
	err = json.NewDecoder(resp.Body).Decode(&mainstemResponse)
	if err != nil {
		return "", err
	}
	if mainstemResponse.Type_ != "FeatureCollection" {
		return "", fmt.Errorf("expected FeatureCollection with one item for %s, but got unexpected response type: %s", url, mainstemResponse.Type_)
	}
	if len(mainstemResponse.Features) > 1 {
		return "", fmt.Errorf("expected FeatureCollection with exactly up to one matching comid item for %s, but got unexpected number of items: %d", url, len(mainstemResponse.Features))
	}
	if len(mainstemResponse.Features) == 1 {
		return mainstemResponse.Features[0].Properties.URI, nil
	}
	return "", nil
}

func (r USGSMainstemService) GetMainstemForWkt(wkt string) (MainstemQueryResponse, error) {
	geometry, err := geom.UnmarshalWKT(wkt)
	if err != nil {
		return MainstemQueryResponse{}, err
	}
	point := geometry.Centroid()
	coordinates, isNonEmpty := point.Coordinates()
	if !isNonEmpty {
		return MainstemQueryResponse{}, fmt.Errorf("got an empty centroid result for WKT: %s", wkt)
	}
	return r.getMainstemForPoint(coordinates.X, coordinates.Y)
}

func (r USGSMainstemService) getMainstemForPoint(longitude float64, latitude float64) (MainstemQueryResponse, error) {
	featureId, err := r.getAssociatedCatchment(longitude, latitude)
	if err != nil {
		return MainstemQueryResponse{}, err
	}
	log.Tracef("Got feature with id %d", featureId)
	upstreamFeature, err := r.getUpstreamFeatureOfCatchment(featureId)
	if err != nil {
		return MainstemQueryResponse{}, err
	}
	log.Tracef("Got upstream feature with id %s", upstreamFeature)
	mainstem, err := r.getGeoconnexURIFromComid(upstreamFeature)
	if err != nil {
		return MainstemQueryResponse{}, err
	}
	return MainstemQueryResponse{
		mainstemURI:             mainstem,
		foundAssociatedMainstem: mainstem != "",
	}, nil
}

func (r USGSMainstemService) AddMainstemToJsonLD(jsonldMap map[string]any, mainstemURI string) (map[string]any, error) {
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
