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
	// the url link itself
	Loc        string  `xml:"loc"`
	LastMod    string  `xml:"lastmod"`
	ChangeFreq string  `xml:"changefreq"`
	Priority   float32 `xml:"priority"`

	// A base64 encoded version of the url link loc that can be used as a key for lookups
	Base64Loc string `xml:"-"`
}

func NewUrlFromSitemapEntry(sitemap sitemap.Entry) *URL {

	// need to check this to prevent a nil pointer deref panic
	var lastModified string
	if sitemap.GetLastModified() != nil {
		lastModified = sitemap.GetLastModified().String()
	} else {
		lastModified = ""
	}

	return &URL{
		// trim space just in case the creator of the sitemap
		// messed up and added spaces at the start of the url
		// preventing it from being fetched properly
		Loc:        strings.TrimSpace(sitemap.GetLocation()),
		LastMod:    lastModified,
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
