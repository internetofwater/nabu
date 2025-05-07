// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package config

type NabuConfig struct {
	Minio       MinioConfig
	Sparql      SparqlConfig
	Context     ContextConfig
	ContextMaps []ContextMap
	Prefixes    []string
	Trace       bool
}

type SparqlConfig struct {
	Endpoint     string
	Authenticate bool
	Username     string
	Password     string
	// the name of the repository in graphdb
	Repository string
	// the number of statements to send in one batch
	// when upserting triples
	Batch int
}

type MinioConfig struct {
	Address   string
	Port      int
	Ssl       bool
	Accesskey string
	Secretkey string
	Bucket    string
	Region    string
}

type ContextConfig struct {
	// whether or not to cache the context when
	// decoding json-ld
	Cache  bool
	Strict bool
}

type ContextMap struct {
	Prefix string
	File   string
}
