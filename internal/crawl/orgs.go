// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"html/template"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/config"
)

const orgJSONLDTemplate = `{
		"@context": {
			"@vocab": "https://schema.org/"
		},
		"@id": "https://gleaner.io/id/org/{{.Name}}",
		"@type": "Organization",
		"url": "{{.URL}}",
		"name": "{{.Name}}",
		 "identifier": {
			"@type": "PropertyValue",
			"@id": "{{.PID}}",
			"propertyID": "https://registry.identifiers.org/registry/doi",
			"url": "{{.PID}}",
			"description": "Persistent identifier for this organization"
		}
	}`

// Generate a jsonld file to represent the metadata for
// a particular organization that we crawl
func newOrgsJsonLD(url, name string) (string, error) {

	const pid = "https://gleaner.io/genid/geoconnex"

	t := template.Must(template.New("org").Parse(orgJSONLDTemplate))
	var tpl bytes.Buffer
	if err := t.Execute(&tpl, map[string]string{
		"URL":  url,
		"Name": name,
		"PID":  pid,
	}); err != nil {
		return "", err
	}
	return tpl.String(), nil
}

// Generate an nq file for a single organization
func NewOrgsNq(url, name string) (string, error) {
	jsonld, err := newOrgsJsonLD(url, name)
	if err != nil {
		return "", err
	}
	processor, options, err := common.NewJsonldProcessor(false, []config.ContextMap{})
	if err != nil {
		return "", err
	}
	return common.JsonldToNQ(jsonld, processor, options)
}
