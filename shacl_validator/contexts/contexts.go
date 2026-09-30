// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

// Package contexts bundles remote JSON-LD context documents so that
// validation does not need to fetch them over the network
package contexts

import _ "embed"

// The schema.org JSON-LD context; this is the document that https://schema.org/
// points to with its Link header. Refresh it with:
//
//	curl -sL https://schema.org/docs/jsonldcontext.jsonld -o schemaorg_context.jsonld
//
//go:embed schemaorg_context.jsonld
var SchemaOrgContext []byte

// The urls that resolve to the schema.org context
var SchemaOrgContextUrls = []string{
	"https://schema.org/",
	"https://schema.org",
	"http://schema.org/",
	"http://schema.org",
	"https://schema.org/docs/jsonldcontext.jsonld",
	"http://schema.org/docs/jsonldcontext.jsonld",
}
