// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"

	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"

	"golang.org/x/sync/errgroup"
)

// SitemapIndex is a structure of <sitemapindex>
// https://geoconnex.us/sitemap.xml is an example of a sitemap index
type SitemapIndex struct {
	XMLName xml.Name `xml:"http://www.sitemaps.org/schemas/sitemap/0.9 sitemapindex"`
	// this represents the <sitemap> elements within the sitemap index
	// the info for all the urls in the sitemap itself is in the `Sitemap` struct
	Sitemaps []SitemapInIndex `xml:"sitemap"`

	storageDestination           storage.CrawlStorage `xml:"-"`
	concurrentSitemaps           int                  `xml:"-"`
	specificSourceToHarvest      string               `xml:"-"`
	sitemapWorkers               int                  `xml:"-"`
	headlessChromeUrl            string               `xml:"-"`
	shaclAddress                 string               `xml:"-"`
	outdatedJsonldCleanupEnabled bool                 `xml:"-"`
	exitOnShaclFailure           bool                 `xml:"-"`
}

// sitemap_ is a structure of <sitemap> within a <sitemapindex>
type SitemapInIndex struct {
	Loc       string `xml:"loc"`
	LastMod   string `xml:"lastmod"`
	SitemapID string `xml:"https://geoconnex.us sitemap_id"`
}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func NewSitemapIndexHarvester(sitemapRef string, client *http.Client) (SitemapIndex, error) {

	serializedSitemapIndex := SitemapIndex{}

	var sitemapData io.Reader

	if isUrl(sitemapRef) {

		res, err := client.Get(sitemapRef)
		if err != nil {
			return serializedSitemapIndex, err
		}
		defer func() { _ = res.Body.Close() }()
		sitemapData = res.Body
	} else {
		sitemapFile, err := os.Open(sitemapRef)
		if err != nil {
			return serializedSitemapIndex, err
		}
		defer func() { _ = sitemapFile.Close() }()
		sitemapData = sitemapFile
	}

	asBytes, err := io.ReadAll(sitemapData)
	if err != nil {
		return serializedSitemapIndex, err
	}
	decoder := xml.NewDecoder(bytes.NewReader(asBytes))

	err = decoder.Decode(&serializedSitemapIndex)
	if err != nil {
		return serializedSitemapIndex, err
	}
	if len(serializedSitemapIndex.Sitemaps) == 0 {
		return serializedSitemapIndex, fmt.Errorf("no sitemaps found in sitemap index at %s", sitemapRef)
	}

	return serializedSitemapIndex, err

}

func (i SitemapIndex) GetUrlList() []string {
	result := []string{}
	for _, sitemap := range i.Sitemaps {
		result = append(result, sitemap.Loc)
	}
	return result
}

func (i SitemapIndex) HarvestSitemaps(ctx context.Context, client *http.Client) (pkg.SitemapIndexCrawlStats, error) {

	if i.concurrentSitemaps < 1 {
		return pkg.SitemapIndexCrawlStats{}, fmt.Errorf("concurrent sitemap limit is set less than 1")
	}
	if i.sitemapWorkers < 1 {
		return pkg.SitemapIndexCrawlStats{}, fmt.Errorf("sitemap workers limit is set less than 1")
	}

	var group errgroup.Group
	group.SetLimit(i.concurrentSitemaps)

	var wasFound atomic.Bool
	wasFound.Store(i.specificSourceToHarvest == "")

	crawlStatChan := make(chan pkg.SitemapCrawlStats, len(i.Sitemaps))

	for _, sitemap := range i.Sitemaps {
		group.Go(func() error {

			id := sitemap.SitemapID

			if i.specificSourceToHarvest != "" && id != i.specificSourceToHarvest {
				log.Debugf("Skipped sitemap with id %s", id)
				return nil
			} else {
				wasFound.Store(true)
			}

			log.Infof("Parsing sitemap %s", sitemap.Loc)
			sitemap, err := NewSitemap(ctx, client, sitemap.Loc, i.sitemapWorkers, i.storageDestination, id)
			if err != nil {
				return err
			}
			shaclGRPCClient, err := NewShaclGrpcClientFromAddr(i.shaclAddress)
			if err != nil {
				return err
			}

			config, err := NewSitemapHarvestConfig(client, sitemap, shaclGRPCClient, i.exitOnShaclFailure, i.outdatedJsonldCleanupEnabled)
			if err != nil {
				return err
			}

			stats, _, harvestErr := sitemap.
				Harvest(ctx, &config)

			crawlStatChan <- stats

			return harvestErr
		})
	}

	if err := group.Wait(); err != nil {
		return pkg.SitemapIndexCrawlStats{}, err
	}

	if !wasFound.Load() {
		return pkg.SitemapIndexCrawlStats{}, fmt.Errorf("no sitemap found with id %s", i.specificSourceToHarvest)
	}

	// we close this here to make sure we can range without blocking
	// We know we can close this since we have already waited on all go routines
	close(crawlStatChan)
	allStats := []pkg.SitemapCrawlStats{}
	for stats := range crawlStatChan {
		allStats = append(allStats, stats)
	}

	return allStats, nil
}

// Harvest one particular sitemap
func (i SitemapIndex) HarvestSitemap(ctx context.Context, client *http.Client, sitemapIdentifier string) (pkg.SitemapCrawlStats, error) {

	for _, part := range i.Sitemaps {

		id := part.SitemapID

		if id != sitemapIdentifier {
			continue
		}
		ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapIdentifier))
		defer span.End()

		sitemap, err := NewSitemap(ctx, client, part.Loc, i.sitemapWorkers, i.storageDestination, id)
		if err != nil {
			return pkg.SitemapCrawlStats{}, err
		}

		shaclGRPCClient, err := NewShaclGrpcClientFromAddr(i.shaclAddress)
		if err != nil {
			return pkg.SitemapCrawlStats{}, err
		}

		config, err := NewSitemapHarvestConfig(client, sitemap, shaclGRPCClient, i.exitOnShaclFailure, i.outdatedJsonldCleanupEnabled)

		if err != nil {
			return pkg.SitemapCrawlStats{}, err
		}

		stats, _, err := sitemap.
			Harvest(ctx, &config)
		return stats, err
	}
	return pkg.SitemapCrawlStats{}, fmt.Errorf("sitemap %s not found in sitemap", sitemapIdentifier)
}
