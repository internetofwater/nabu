package gleaner

import (
	"encoding/xml"
	"fmt"
	"nabu/internal/common"
	"strings"
	"time"

	crawl "nabu/internal/crawl"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"golang.org/x/sync/errgroup"
)

// Represents an XML sitemap
type Sitemap struct {
	XMLName xml.Name `xml:":urlset"`
	URL     []URL    `xml:":url"`

	// Strategy used for storing crawled data
	// - explicitly ignores xml marshaling
	// since this is not an xml field but rather
	// associated data with the sitemap struct
	storageDestination crawl.CrawlStorage `xml:"-"`
}

// Set the storage strategy for the struct
func (s Sitemap) SetStorageDestination(storageDestination crawl.CrawlStorage) Sitemap {
	s.storageDestination = storageDestination
	return s
}

// Harvest all the URLs in the sitemap
func (s Sitemap) Harvest(workers int) error {
	var group errgroup.Group
	group.SetLimit(workers)

	// For the time being, we assume that the first URL in the sitemap has the
	// same robots.txt as the rest of the items
	if len(s.URL) == 0 {
		return fmt.Errorf("no URLs found in sitemap")
	} else if s.storageDestination == nil {
		return fmt.Errorf("no storage destination set")
	} else if workers < 1 {
		return fmt.Errorf("no workers set")
	}

	firstUrl := s.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return err
	}
	if !robotstxt.Test(gleanerAgent) {
		return fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
	}

	client := common.NewRetryableHTTPClient()
	for _, url := range s.URL {
		url := url
		group.Go(func() error {
			resp, err := client.Get(url.Loc)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				return fmt.Errorf("failed to fetch %s, got status %s", url.Loc, resp.Status)
			}

			// To generate a hash we need to copy the response body
			respBodyCopy, itemHash, err := copyReaderAndGenerateHashFilename(resp.Body)
			if err != nil {
				return err
			}

			exists, err := s.storageDestination.Exists(itemHash)
			if err != nil {
				return err
			}

			if !exists {
				// Store from the buffered copy
				if err = s.storageDestination.Store(itemHash, respBodyCopy); err != nil {
					return err
				}
			}

			if robotstxt.CrawlDelay > 0 {
				time.Sleep(robotstxt.CrawlDelay)
			}
			return nil
		})
	}
	return group.Wait()
}

// Represents a URL tag and its attributes within a sitemap
type URL struct {
	Loc        string  `xml:"loc"`
	LastMod    string  `xml:"lastmod"`
	ChangeFreq string  `xml:"changefreq"`
	Priority   float32 `xml:"priority"`
}

// Given a sitemap url, return a Sitemap object
func NewSitemap(sitemapURL string) (Sitemap, error) {
	serializedSitemap := Sitemap{}

	urls := make([]URL, 0)
	err := sitemap.ParseFromSite(sitemapURL, func(entry sitemap.Entry) error {
		url := URL{}
		url.Loc = strings.TrimSpace(entry.GetLocation())
		urls = append(urls, url)
		return nil
	})

	if err != nil {
		return serializedSitemap, err
	}

	serializedSitemap.URL = urls
	return serializedSitemap, nil
}
