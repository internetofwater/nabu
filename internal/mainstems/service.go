// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"context"
	"errors"
	"fmt"

	"github.com/peterstace/simplefeatures/geom"
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
	GetMainstemForWkt(ctx context.Context, wkt string) (MainstemQueryResponse, error)
}

// An error type representing that the client
// tried to pass an invalid WKT string to the mainstem service
type InvalidWktError struct {
	message string
}

func (e *InvalidWktError) Error() string {
	return e.message
}

// the largest WKT geometry that will be included in a log or error message;
// log collectors split very long lines into multiple log entries, so larger
// geometries are omitted entirely instead
const maxLoggedWktBytes = 2048

// describe a WKT geometry for a log or error message, omitting it if it is too large
func wktForLog(wkt string) string {
	if len(wkt) <= maxLoggedWktBytes {
		return wkt
	}
	return fmt.Sprintf("<geometry omitted, %d bytes of WKT>", len(wkt))
}

// Get the uri of the mainstem associated with a WKB geometry. An empty string is returned
// without an error if there is no geometry, the geometry is invalid, or there is no associated mainstem.
// The featureID identifies the feature the geometry belongs to in log messages
func GetMainstemURIForWkb(ctx context.Context, service MainstemService, featureID string, wkb []byte) (string, error) {
	if len(wkb) == 0 {
		return "", nil
	}
	geometry, err := geom.UnmarshalWKB(wkb, geom.NoValidate{})
	if err != nil {
		log.Errorf("Could not parse the WKB geometry of %s to find its mainstem: %v", featureID, err)
		return "", nil
	}
	wkt := geometry.AsText()
	response, err := service.GetMainstemForWkt(ctx, wkt)
	var invalidWktErr *InvalidWktError
	if errors.As(err, &invalidWktErr) {
		log.Errorf("Invalid geometry %s of %s could not be associated with a mainstem: %v", wktForLog(wkt), featureID, err)
		return "", nil
	} else if err != nil {
		return "", err
	}
	if !response.foundAssociatedMainstem {
		log.Debugf("no mainstem found for %s with geometry %s", featureID, wktForLog(wkt))
		return "", nil
	}
	return response.mainstemURI, nil
}
