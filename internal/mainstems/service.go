// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package mainstems

import (
	"context"
	"errors"

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

// Get the uri of the mainstem associated with a WKB geometry. An empty string is returned
// without an error if there is no geometry, the geometry is invalid, or there is no associated mainstem
func GetMainstemURIForWkb(ctx context.Context, service MainstemService, wkb []byte) (string, error) {
	if len(wkb) == 0 {
		return "", nil
	}
	geometry, err := geom.UnmarshalWKB(wkb, geom.NoValidate{})
	if err != nil {
		log.Errorf("Could not parse WKB geometry to find its mainstem: %v", err)
		return "", nil
	}
	wkt := geometry.AsText()
	response, err := service.GetMainstemForWkt(ctx, wkt)
	var invalidWktErr *InvalidWktError
	if errors.As(err, &invalidWktErr) {
		log.Errorf("Invalid geometry %s could not be associated with a mainstem: %v", wkt, err)
		return "", nil
	} else if err != nil {
		return "", err
	}
	if !response.foundAssociatedMainstem {
		log.Debugf("no mainstem found for %s", wkt)
		return "", nil
	}
	return response.mainstemURI, nil
}
