// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"fmt"
	"nabu/internal/common"
	"nabu/internal/crawl/storage"
	"nabu/internal/opentelemetry"
	"net/http"
	"strings"
	"time"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	log "github.com/sirupsen/logrus"
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
	storageDestination storage.CrawlStorage `xml:"-"`
}

// Set the storage strategy for the struct
func (s Sitemap) SetStorageDestination(storageDestination storage.CrawlStorage) Sitemap {
	s.storageDestination = storageDestination
	return s
}

// Harvest all the URLs in the sitemap
func (s Sitemap) Harvest(ctx context.Context, workers int, outputFoldername string) error {

	span, _ := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", outputFoldername))
	defer span.End()

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

			req, err := http.NewRequest("GET", url.Loc, nil)
			if err != nil {
				return err
			}
			req.Header.Set("User-Agent", gleanerAgent)
			req.Header.Set("Accept", "application/ld+json")

			resp, err := client.Do(req)
			if err != nil {
				return err
			}

			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				log.Errorf("failed to fetch %s, got status %s", url.Loc, resp.Status)
				return nil
			}

			// To generate a hash we need to copy the response body
			respBodyCopy, itemHash, err := copyReaderAndGenerateHashFilename(resp.Body)
			if err != nil {
				return err
			}

			summonedPath := fmt.Sprintf("summoned/%s/%s", outputFoldername, itemHash)

			exists, err := s.storageDestination.Exists(summonedPath)
			if err != nil {
				return err
			}

			if !exists {

				// Store from the buffered copy
				if err = s.storageDestination.Store(summonedPath, respBodyCopy); err != nil {
					return err
				}
				log.Debugf("stored %s as %s", url.Loc, itemHash)
			} else {
				log.Debugf("%s already exists so skipping", url.Loc)
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
func NewSitemap(ctx context.Context, sitemapURL string) (Sitemap, error) {
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
