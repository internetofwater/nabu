// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"golang.org/x/sync/errgroup"
)

// Index is a structure of <sitemapindex>
type Index struct {
	XMLName  xml.Name `xml:"sitemapindex"`
	Sitemaps []parts  `xml:"sitemap"`

	storageDestination      storage.CrawlStorage `xml:"-"`
	concurrentSitemaps      int                  `xml:"-"`
	specificSourceToHarvest string               `xml:"-"`
	sitemapWorkers          int                  `xml:"-"`
	headlessChromeUrl       string               `xml:"-"`
	shaclValidationEnabled  bool                 `xml:"-"`
}

// parts is a structure of <sitemap> in <sitemapindex>
type parts struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func (p parts) associatedID() (string, error) {
	if p.Loc == "" {
		return "", fmt.Errorf("empty sitemap location")
	}

	url, err := url.Parse(p.Loc)
	if err != nil {
		return "", err
	}
	path := strings.TrimPrefix(url.Path, "/sitemap/")
	removeXML := strings.TrimSuffix(path, ".xml")
	underscoredPath := strings.ReplaceAll(removeXML, "/", "_")
	return underscoredPath, nil
}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func NewSitemapIndexHarvester(sitemapRef string) (Index, error) {

	serializedSitemapIndex := Index{}

	var sitemapData io.Reader

	if isUrl(sitemapRef) {
		// For some reason without this the server sometimes
		// returns EOF
		client := http.Client{
			Timeout: 10 * time.Second,
		}

		res, err := client.Get(sitemapRef)
		if err != nil {
			return serializedSitemapIndex, err
		}
		defer res.Body.Close()
		sitemapData = res.Body
	} else {
		sitemapFile, err := os.Open(sitemapRef)
		if err != nil {
			return serializedSitemapIndex, err
		}
		defer sitemapFile.Close()
		sitemapData = sitemapFile
	}

	err := sitemap.ParseIndex(sitemapData, func(ie sitemap.IndexEntry) error {
		part := parts{}
		part.Loc = strings.TrimSpace(ie.GetLocation())
		part.LastMod = ie.GetLastModified().Format(time.RFC3339)
		serializedSitemapIndex.Sitemaps = append(serializedSitemapIndex.Sitemaps, part)
		return nil
	})
	if len(serializedSitemapIndex.Sitemaps) == 0 {
		return serializedSitemapIndex, fmt.Errorf("%s appears to be empty or an invalid sitemap index", sitemapRef)
	}

	return serializedSitemapIndex, err

}

func (i Index) GetUrlList() []string {
	result := []string{}
	for _, part := range i.Sitemaps {
		result = append(result, part.Loc)
	}
	return result
}

func (i Index) HarvestSitemaps(ctx context.Context) ([]SitemapCrawlStats, error) {
	var group errgroup.Group
	group.SetLimit(i.concurrentSitemaps)

	var wasFound atomic.Bool
	wasFound.Store(i.specificSourceToHarvest == "")

	crawlStatChan := make(chan SitemapCrawlStats, len(i.Sitemaps))

	for _, part := range i.Sitemaps {
		part := part
		group.Go(func() error {
			id, err := part.associatedID()
			if err != nil {
				return err
			}

			if i.specificSourceToHarvest != "" && id != i.specificSourceToHarvest {
				return nil
			} else {
				wasFound.Store(true)
			}

			sitemap, err := NewSitemap(ctx, part.Loc)
			if err != nil {
				return err
			}

			nq, err := NewOrgsNq(part.Loc, part.Loc)
			if err != nil {
				return err
			}

			prov, err := ProvData{SOURCE: part.Loc}.toNq()
			if err != nil {
				return err
			}

			const metadataFiles = 2
			errChan := make(chan error, metadataFiles)
			go func() {
				errChan <- i.storageDestination.Store("orgs/"+id+".nq", strings.NewReader(nq))
				errChan <- i.storageDestination.Store("prov/"+id+".nq", strings.NewReader(prov))
				close(errChan)
			}()

			stats, harvestErr := sitemap.SetStorageDestination(i.storageDestination).
				Harvest(ctx, i.sitemapWorkers, id, i.shaclValidationEnabled)

			for err := range errChan {
				if err != nil {
					return err
				}
			}

			crawlStatChan <- stats

			return harvestErr
		})
	}

	if err := group.Wait(); err != nil {
		return []SitemapCrawlStats{}, err
	}

	if !wasFound.Load() {
		return []SitemapCrawlStats{}, fmt.Errorf("no sitemap found with id %s", i.specificSourceToHarvest)
	}

	// we close this here to make sure we can range without blocking
	// We know we can close this since we have already waited on all go routines
	close(crawlStatChan)
	allStats := []SitemapCrawlStats{}
	for stats := range crawlStatChan {
		allStats = append(allStats, stats)
	}

	return allStats, nil
}

// Harvest one particular sitemap
func (i Index) HarvestSitemap(ctx context.Context, sitemapIdentifier string) (SitemapCrawlStats, error) {

	for _, part := range i.Sitemaps {

		id, err := part.associatedID()
		if err != nil {
			return SitemapCrawlStats{}, err
		}

		if id != sitemapIdentifier {
			continue
		}
		ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapIdentifier))
		defer span.End()

		sitemap, err := NewSitemap(ctx, part.Loc)
		if err != nil {
			return SitemapCrawlStats{}, err
		}
		return sitemap.SetStorageDestination(i.storageDestination).
			Harvest(ctx, i.sitemapWorkers, id, i.shaclValidationEnabled)
	}
	return SitemapCrawlStats{}, fmt.Errorf("sitemap %s not found in sitemap", sitemapIdentifier)
}
