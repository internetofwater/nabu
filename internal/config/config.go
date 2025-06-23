// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package config

// The top level config for all nabu operations
type NabuConfig struct {
	Minio             MinioConfig
	Sparql            SparqlConfig
	Context           ContextConfig
	PrefixToFileCache map[string]string
	Prefixes          []string
	Trace             bool
}

// The config for sparql and graph interactions
type SparqlConfig struct {
	Endpoint   string `arg:"--endpoint" help:"endpoint for server for the SPARQL endpoints" default:"http://127.0.0.1:7200"`
	Repository string `arg:"--repository" help:"the default repository to use for graphdb" default:"iow"` // the default repository to use for graphdb
	// the number of statements to send in one batch
	// when upserting triples
	UpsertBatchSize int `arg:"--upsert-batch-size" default:"1"`
}

// The config for minio/s3 operations
type MinioConfig struct {
	Address   string `arg:"--address" help:"The address of the s3 server" default:"127.0.0.1"` // The address of the minio server
	Port      int    `arg:"--port" default:"9000"`
	Accesskey string `arg:"--s3-access-key,env:S3_ACCESS_KEY" help:"Access Key (i.e. username)" default:"minioadmin"` // Access Key (i.e. username)
	Secretkey string `arg:"--s3-secret-key,env:S3_SECRET_KEY" help:"Secret Key (i.e. password)" default:"minioadmin"` // Secret Key (i.e. password)
	Bucket    string `arg:"--bucket" help:"The s3 bucket to use for sync operations" default:"iow"`                   // The configuration bucket
	Region    string `arg:"--region" help:"region for the s3 server"`                                                 // region for the minio server
	SSL       bool   `arg:"--ssl" help:"Use SSL when connecting to s3"`
}

// THe config for jsonld context operations
type ContextConfig struct {
	// whether or not to cache the context when
	// decoding json-ld
	Cache  bool `arg:"--cache" help:"use cache for context"`
	Strict bool `arg:"--strict" help:"use strict mode for context"`
}
