// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"html/template"
	"io"
	"nabu/internal/common"
	"nabu/internal/config"
)

// Holds the prov meatdata data for a summoned data graph
type ProvData struct {
	RESID  string
	SHA    string
	PID    string
	SOURCE string
	DATE   string
	RUNID  string
	URN    string
	PNAME  string
	DOMAIN string
}

func (p ProvData) toJsonLD() io.Reader {
	t := template.Must(template.New("prov").Parse(provTemplate))
	var tpl bytes.Buffer
	if err := t.Execute(&tpl, p); err != nil {
		return bytes.NewBufferString(err.Error())
	}
	return &tpl
}

func (p ProvData) toNq() (string, error) {
	jsonld := p.toJsonLD().(*bytes.Buffer).String()
	processor, options, err := common.NewJsonldProcessor(false, []config.ContextMap{})
	if err != nil {
		return "", err
	}
	return common.JsonldToNQ(jsonld, processor, options)
}

var provTemplate = `{
	"@context": {
	  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	  "prov": "http://www.w3.org/ns/prov#",
	  "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
	},
	"@graph": [
	  {
		"@id": "{{.PID}}",
		"@type": "prov:Organization",
		"rdf:name": "{{.PNAME}}",
		"rdfs:seeAlso": "{{.DOMAIN}}"
	  },
	  {
		"@id": "{{.RESID}}",
		"@type": "prov:Entity",
		"prov:wasAttributedTo": {
		  "@id": "{{.PID}}"
		},
		"prov:value": "{{.RESID}}"
	  },
	  {
		"@id": "https://gleaner.io/id/collection/{{.SHA}}",
		"@type": "prov:Collection",
		"prov:hadMember": {
		  "@id": "{{.RESID}}"
		}
	  },
	  {
		"@id": "{{.URN}}",
		"@type": "prov:Entity",
		"prov:value": "{{.SHA}}.jsonld"
	  },
	  {
		"@id": "https://gleaner.io/id/run/{{.SHA}}",
		"@type": "prov:Activity",
		"prov:endedAtTime": {
		  "@value": "{{.DATE}}",
		  "@type": "http://www.w3.org/2001/XMLSchema#dateTime"
		},
		"prov:generated": {
		  "@id": "{{.URN}}"
		},
		"prov:used": {
		  "@id": "https://gleaner.io/id/collection/{{.SHA}}"
		}
	  }
	]
  }`
