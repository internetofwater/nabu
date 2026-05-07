// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"github.com/internetofwater/nabu/internal/crawl/storage"

	log "github.com/sirupsen/logrus"
)

func (i SitemapIndex) WithStorageDestination(storageDestination storage.CrawlStorage) SitemapIndex {
	i.storageDestination = storageDestination
	return i
}

func (i SitemapIndex) WithShaclValidationConfig(shaclAddress string, exitOnShaclFailure bool) SitemapIndex {
	i.shaclAddress = shaclAddress
	i.exitOnShaclFailure = exitOnShaclFailure
	return i
}

func (i SitemapIndex) WithOutdatedJsonldCleanup(enabled bool) SitemapIndex {
	i.outdatedJsonldCleanupEnabled = enabled
	return i
}

func (i SitemapIndex) WithConcurrencyConfig(concurrentSitemaps int, sitemapWorkers int) SitemapIndex {
	// Make sure concurrency is at least 1
	// otherwise go will block indefinitely
	if concurrentSitemaps < 1 {
		log.Warnf("concurrency val is set to %d which is less than 1, so setting to 1", concurrentSitemaps)
		concurrentSitemaps = 1
	}
	if sitemapWorkers < 1 {
		log.Warnf("worker val is set to %d which is less than 1, so setting to 1", sitemapWorkers)
		sitemapWorkers = 1
	}

	i.concurrentSitemaps = concurrentSitemaps
	i.sitemapWorkers = sitemapWorkers
	return i
}

func (i SitemapIndex) WithSpecifiedSourceFilter(sourceToHarvest string) SitemapIndex {
	// Set an id to filter by
	// If a sitemap with this id is found, it will be harvested
	// otherwise it will be skipped. If the id is an empty string
	// it will harvest all sitemaps
	i.specificSourceToHarvest = sourceToHarvest
	return i
}

func (i SitemapIndex) WithHeadlessChromeUrl(url string) SitemapIndex {
	i.headlessChromeUrl = url
	return i
}
