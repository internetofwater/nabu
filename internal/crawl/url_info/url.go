// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package url_info

import (
	"encoding/base64"
	"strings"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
)

// Represents a URL tag and its attributes within a sitemap
type URL struct {
	Loc        string  `xml:"loc"`
	LastMod    string  `xml:"lastmod"`
	ChangeFreq string  `xml:"changefreq"`
	Priority   float32 `xml:"priority"`

	// A base64 encoded version of the loc that can be used as a key for lookups
	Base64Loc string `xml:"-"`
}

func NewUrlFromSitemapEntry(sitemap sitemap.Entry) *URL {

	return &URL{
		// trim space just in case the creator of the sitemap
		// messed up and added spaces at the start of the url
		// preventing it from being fetched properly
		Loc:        strings.TrimSpace(sitemap.GetLocation()),
		LastMod:    sitemap.GetLastModified().String(),
		ChangeFreq: sitemap.GetChangeFrequency(),
		Priority:   sitemap.GetPriority(),
		Base64Loc:  base64.StdEncoding.EncodeToString([]byte(sitemap.GetLocation())),
	}
}

// Return a URL without the rest of the sitemap fields
func NewUrlFromString(url string) URL {
	return URL{
		Loc:       url,
		Base64Loc: base64.StdEncoding.EncodeToString([]byte(url)),
	}
}
