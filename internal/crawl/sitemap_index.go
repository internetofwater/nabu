// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"fmt"
	"nabu/internal/interfaces"
	"nabu/internal/opentelemetry"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"golang.org/x/sync/errgroup"
)

// Index is a structure of <sitemapindex>
type Index struct {
	XMLName  xml.Name `xml:"sitemapindex"`
	Sitemaps []parts  `xml:"sitemap"`

	storageDestination      interfaces.CrawlStorage `xml:"-"`
	concurrentSitemaps      int                     `xml:"-"`
	specificSourceToHarvest string                  `xml:"-"`
	sitemapWorkers          int                     `xml:"-"`
}

// parts is a structure of <sitemap> in <sitemapindex>
type parts struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func (p parts) associatedID() (string, error) {
	url, err := url.Parse(p.Loc)
	if err != nil {
		return "", err
	}
	segments := strings.Split(strings.Trim(url.Path, "/"), "/")
	if len(segments) == 0 {
		return "", fmt.Errorf("path does not contain any segments")
	}
	last := segments[len(segments)-1]
	ext := filepath.Ext(last)
	if ext == "" {
		return "", fmt.Errorf("no file extension found in path")
	}
	id := strings.TrimSuffix(last, ext)
	return id, nil
}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func NewSitemapIndexHarvester(sitemapRef string) (Index, error) {

	serializedSitemapIndex := Index{}

	if isUrl(sitemapRef) {
		return serializedSitemapIndex, sitemap.ParseIndexFromSite(sitemapRef, func(ie sitemap.IndexEntry) error {
			part := parts{}
			part.Loc = strings.TrimSpace(ie.GetLocation())
			part.LastMod = ie.GetLastModified().Format(time.RFC3339)
			serializedSitemapIndex.Sitemaps = append(serializedSitemapIndex.Sitemaps, part)
			return nil
		})
	} else {
		return serializedSitemapIndex, sitemap.ParseIndexFromFile(sitemapRef, func(ie sitemap.IndexEntry) error {
			part := parts{}
			part.Loc = strings.TrimSpace(ie.GetLocation())
			part.LastMod = ie.GetLastModified().Format(time.RFC3339)
			serializedSitemapIndex.Sitemaps = append(serializedSitemapIndex.Sitemaps, part)
			return nil
		})
	}

}

func (i Index) GetUrlList() []string {
	result := []string{}
	for _, part := range i.Sitemaps {
		result = append(result, part.Loc)
	}
	return result
}

// Harvest all the URLs in the sitemap
func (i Index) HarvestSitemaps(ctx context.Context) error {

	var group errgroup.Group
	group.SetLimit(i.concurrentSitemaps)

	for _, part := range i.Sitemaps {
		part := part
		group.Go(func() error {

			id, err := part.associatedID()
			if err != nil {
				return err
			}
			// If we are filtering by a specific source, skip all others
			if i.specificSourceToHarvest != "" && id != i.specificSourceToHarvest {
				return nil
			}

			sitemap, err := NewSitemap(ctx, part.Loc)
			if err != nil {
				return err
			}

			nq, err := NewOrgsNq(part.Loc, part.Loc)
			if err != nil {
				return err
			}

			errChan := make(chan error, 1)
			go func(id string) {
				errChan <- i.storageDestination.Store("orgs/"+id+".nq", strings.NewReader(nq))
			}(id)

			harvestResult := sitemap.SetStorageDestination(i.storageDestination).
				Harvest(ctx, i.sitemapWorkers, id)

			if err := <-errChan; err != nil {
				return err
			}
			return harvestResult
		})
	}

	return group.Wait()
}

// Harvest one particular sitemap
func (i Index) HarvestSitemap(ctx context.Context, sitemapIdentifier string) error {

	for _, part := range i.Sitemaps {

		id, err := part.associatedID()
		if err != nil {
			return err
		}

		if id != sitemapIdentifier {
			continue
		}
		span, ctx := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapIdentifier))
		defer span.End()

		sitemap, err := NewSitemap(ctx, part.Loc)
		if err != nil {
			return err
		}
		return sitemap.SetStorageDestination(i.storageDestination).
			Harvest(ctx, i.sitemapWorkers, id)
	}
	return fmt.Errorf("sitemap %s not found in sitemap", sitemapIdentifier)
}

func (i Index) WithStorageDestination(storageDestination interfaces.CrawlStorage) Index {
	i.storageDestination = storageDestination
	return i
}

func (i Index) WithConcurrencyConfig(concurrentSitemaps int, sitemapWorkers int) Index {
	// Make sure concurrency is at least 1
	// otherwise go will block indefinitely
	if concurrentSitemaps < 1 {
		concurrentSitemaps = 1
	}
	if sitemapWorkers < 1 {
		sitemapWorkers = 1
	}

	i.concurrentSitemaps = concurrentSitemaps
	i.sitemapWorkers = sitemapWorkers
	return i
}

func (i Index) WithSpecifiedSourceFilter(sourceToHarvest string) Index {
	// Set an id to filter by
	// If a sitemap with this id is found, it will be harvested
	// otherwise it will be skipped. If the id is an empty string
	// it will harvest all sitemaps
	i.specificSourceToHarvest = sourceToHarvest
	return i
}
