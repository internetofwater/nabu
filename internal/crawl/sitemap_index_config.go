// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"nabu/internal/crawl/storage"

	log "github.com/sirupsen/logrus"
)

func (i Index) WithStorageDestination(storageDestination storage.CrawlStorage) Index {
	i.storageDestination = storageDestination
	return i
}

// Set the storage strategy for the struct
func (s Sitemap) SetStorageDestination(storageDestination storage.CrawlStorage) Sitemap {
	s.storageDestination = storageDestination
	return s
}

func (i Index) WithConcurrencyConfig(concurrentSitemaps int, sitemapWorkers int) Index {
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

func (i Index) WithSpecifiedSourceFilter(sourceToHarvest string) Index {
	// Set an id to filter by
	// If a sitemap with this id is found, it will be harvested
	// otherwise it will be skipped. If the id is an empty string
	// it will harvest all sitemaps
	i.specificSourceToHarvest = sourceToHarvest
	return i
}

func (i Index) WithHeadlessChromeUrl(url string) Index {
	i.headlessChromeUrl = url
	return i
}
