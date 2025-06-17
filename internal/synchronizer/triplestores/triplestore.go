// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestores

import (
	"context"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
)

// assert that the graphdb client implements the interface
var _ GenericTriplestore = &GraphDbClient{}

// The set of methods that must be implemented by a triplestore to be used by nabu
type RequiredTriplestoreFeatures interface {
	// Inserts data into a specified named graph.
	UpsertNamedGraphs(ctx context.Context, graphs []common.NamedGraph) error

	// ClearAllGraphs clears all graphs in the triplestore.
	ClearAllGraphs() error

	// Checks if a specified graph exists in the triplestore.
	GraphExists(ctx context.Context, graph string) (bool, error)

	// Removes a specified graph from the triplestore.
	DropGraphs(ctx context.Context, graphs []string) error

	// Returns a list of graphs associated with a given s3 prefix
	NamedGraphsAssociatedWithS3Prefix(ctx context.Context, prefix s3.S3Prefix) ([]string, error)

	// Returns the base url of the triplestore
	GetBaseUrl() string

	// Return the url for rest API queries
	GetRestUrl() string

	// Return the url endpoint for sparql queries
	GetSparqlQueryUrl() string
}

type GenericTriplestore interface {
	// all methods that a triplestore must implement
	RequiredTriplestoreFeatures
}
