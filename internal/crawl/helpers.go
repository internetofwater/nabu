// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"net/url"

	"github.com/internetofwater/nabu/internal/common"

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

	resp, err := common.NewRetryableHTTPClient().Get(robotsUrl)
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
