// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"golang.org/x/net/html"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/temoto/robotstxt"
)

// Given a url, strip off the end and just return the hostname with the
// proper protocol
func getHostname(urlToCheck string) (string, error) {
	parsedURL, err := url.Parse(urlToCheck)
	if err != nil {
		return "", err
	}
	return parsedURL.Scheme + "://" + parsedURL.Host, nil
}

// Create a new robots.txt object from a remote url
// this can be used to check if we are allowed to crawl
func newRobots(httpClient *http.Client, urlToCheck string) (*robotstxt.Group, error) {

	basename, err := getHostname(urlToCheck)
	if err != nil {
		return nil, err
	}

	robotsUrl := basename + "/robots.txt"

	resp, err := httpClient.Get(robotsUrl)
	if err != nil {
		return nil, err
	}

	robots, err := robotstxt.FromResponse(resp)
	if err != nil {
		return nil, err
	}
	return robots.FindGroup(common.HarvestAgent), nil
}

func GetJsonLDFromHTML(data []byte) (string, error) {
	document, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	head := findHead(document)

	scripts := getScriptTags(head)
	for _, s := range scripts {
		for _, attr := range s.Attr {
			// We get the first one, since there should only be one
			if attr.Key == "type" && strings.Contains(attr.Val, "application/ld+json") {
				return s.FirstChild.Data, nil
			}
		}
	}

	return "", fmt.Errorf("no JSON-LD found in document")
}

// Recursively search for the <head> element
func findHead(n *html.Node) *html.Node {
	if n.Type == html.ElementNode && n.Data == "head" {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if result := findHead(c); result != nil {
			return result
		}
	}
	return nil
}

// Collect all <script> nodes under the given node
func getScriptTags(n *html.Node) []*html.Node {
	var scripts []*html.Node
	var traverser func(*html.Node)
	traverser = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "script" {
			scripts = append(scripts, n)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverser(c)
		}
	}
	traverser(n)
	return scripts
}

// A helper for tracking the status of a sitemap
// if we find that the sitemap has failed more than
// the threshold without a single success, then
// we heuristically assume the sitemap is down
// and can use this info to decide whether to crawl further
type SitemapStatusTracker struct {
	failures              atomic.Int32
	foundSuccessful       atomic.Bool
	maxFailuresBeforeDown int
}

// Create a new SitemapStatusTracker
func NewSitemapStatusTracker(maxFailures int) *SitemapStatusTracker {
	return &SitemapStatusTracker{
		maxFailuresBeforeDown: maxFailures,
	}
}

func (s *SitemapStatusTracker) AddSiteFailure() {
	s.failures.Add(1)
}

func (s *SitemapStatusTracker) AddSiteSuccess() {
	s.foundSuccessful.Store(true)
}

type SitemapAppearsDownError struct {
	message string
}

func (s SitemapAppearsDownError) Error() string {
	return s.message
}

var _ error = SitemapAppearsDownError{}

func (s *SitemapStatusTracker) AppearsDown() bool {
	// if the threshold is 0, then we never assume the sitemap is down
	// since we have no threshold to compare against
	if s.maxFailuresBeforeDown == 0 {
		return false
	}

	// if we found a site that is successful
	// that means the sitemap as a whole cannot be down
	if s.foundSuccessful.Load() {
		return false
	}

	// if we've failed more than the threshold, then
	// we heuristically assume the sitemap is down
	return s.failures.Load() >= int32(s.maxFailuresBeforeDown)
}
