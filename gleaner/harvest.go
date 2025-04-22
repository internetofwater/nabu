package gleaner

import (
	"fmt"
	"nabu/internal/common"
	"net/url"

	"github.com/temoto/robotstxt"
)

const gleanerAgent = "gleaner"

var namespaceToRobots = map[string]bool{}

// Given a url, strip off the end and just return the hostname with the
// proper protocol
func getHostname(url_to_check string) (string, error) {
	parsedURL, err := url.Parse(url_to_check)
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

func harvest_url(url string) error {

	robotstxt, err := newRobots(url)
	if err != nil {
		return err
	}

	if !robotstxt.Test(gleanerAgent) {
		return fmt.Errorf("robots.txt does not allow us to crawl %s", url)
	}

	return nil
}
