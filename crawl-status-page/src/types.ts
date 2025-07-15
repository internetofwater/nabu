/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

// The status of the shacl validation
type ShaclStatus = "skipped" | "invalid" | "valid";

interface UrlCrawlError {
  // The URL that failed
  Url: string;
  // The http status code of the fetched Url
  Status: number;
  // A natural language error message describing the error
  Message: string;
  // The status of the shacl validation operation itself
  ShaclStatus: ShaclStatus;
  // The shacl validation message
  ShaclErrorMessage: string;
}

interface SitemapCrawlStats {
  // All the urls that were successfully crawled
  SuccessfulUrls: string[];
  // Metadata about why a sitemap failed to be harvested
  CrawlFailures: UrlCrawlError[] | null;
  // The number of seconds it took to crawl the sitemap
  SecondsToComplete: number;
  // The name of the sitemap in the sitemap index
  SitemapName: string;
  // The number of sites that were successfully crawled and stored
  SitesHarvested: number;
  // The number of total sites in the sitemap
  SitesInSitemap: number;
}

// A sitemap index is just a list of sitemaps
export type SitemapIndexCrawlStats = SitemapCrawlStats[];
