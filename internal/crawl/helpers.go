// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/url"
	"strings"

	"golang.org/x/net/html"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/temoto/robotstxt"
)

const gleanerAgent = "gleaner"

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
func newRobots(urlToCheck string) (*robotstxt.Group, error) {

	basename, err := getHostname(urlToCheck)
	if err != nil {
		return nil, err
	}

	robotsUrl := basename + "/robots.txt"

	resp, err := common.NewCrawlerClient().Get(robotsUrl)
	if err != nil {
		return nil, err
	}

	robots, err := robotstxt.FromResponse(resp)
	if err != nil {
		return nil, err
	}
	return robots.FindGroup(gleanerAgent), nil
}

func generateHashFilename(data []byte) (string, error) {

	hasher := md5.New()
	if _, err := io.Copy(hasher, bytes.NewReader(data)); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x.jsonld", hasher.Sum(nil)), nil
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
