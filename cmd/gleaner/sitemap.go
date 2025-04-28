package gleaner

import (
	"encoding/xml"
	"fmt"
	"nabu/internal/common"
	"strings"
	"time"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
)

// Index is a structure of <sitemapindex>
type Index struct {
	XMLName xml.Name `xml:"sitemapindex"`
	Sitemap []parts  `xml:"sitemap"`
}

// parts is a structure of <sitemap> in <sitemapindex>
type parts struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

// Represents an XML sitemap
type Sitemap struct {
	XMLName xml.Name `xml:":urlset"`
	URL     []URL    `xml:":url"`

	// Strategy used for storing crawled data
	// - explicitly ignores xml marshaling
	// since this is not an xml field but rather
	// associated data with the sitemap struct
	storageDestination CrawlStorage `xml:"-"`
}

// Set the storage strategy for the struct
func (s Sitemap) WithStorageType(storageDestination CrawlStorage) Sitemap {
	s.storageDestination = storageDestination
	return s
}

// Harvest all the URLs in the sitemap
func (s Sitemap) Harvest(workers int) []error {
	var group MultiErrGroup
	group.SetLimit(workers)

	// For the time being, we assume that the first URL in the sitemap has the
	// same robots.txt as the rest of the items
	if len(s.URL) == 0 {
		return []error{fmt.Errorf("no URLs found in sitemap")}
	} else if s.storageDestination == nil {
		return []error{fmt.Errorf("no storage destination set")}
	}

	firstUrl := s.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return []error{err}
	}
	if !robotstxt.Test(gleanerAgent) {
		return []error{fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)}
	}

	client := common.NewRetryableHTTPClient()
	for _, url := range s.URL {
		group.Go(func() error {
			resp, err := client.Get(url.Loc)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				return fmt.Errorf("failed to fetch %s, got status %s", url.Loc, resp.Status)
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

// // This function takes a top level sitemap index like geoconnex.us/sitemap.xml and returns a list of sitemap urls
// A sitemap index is a file that lists the URLs for multiple sitemaps
func GetSitemapListFromIndex(sitemapURL string) ([]string, error) {
	result := []string{}
	err := sitemap.ParseIndexFromSite(sitemapURL, func(e sitemap.IndexEntry) error {
		result = append(result, strings.TrimSpace(e.GetLocation()))
		return nil
	})

	return result, err
}
