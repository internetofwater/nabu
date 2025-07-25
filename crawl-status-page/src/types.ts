/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import vocab from "./vocab.json";

// The status of the shacl validation
type ShaclStatus = "skipped" | "invalid" | "valid";

export interface UrlCrawlError {
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

export interface SitemapCrawlStats {
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

  // The id is a unique identifier for the sitemap
  // that is optional and only set in jsonld
  "@id"?: string;

  // The last modified date of the sitemap
  LastModified?: string;
}

// A sitemap index is just a list of sitemaps
export type SitemapIndexCrawlStats = SitemapCrawlStats[];

type VocabType = typeof vocab

export interface JsonLdReport extends VocabType {
  "@graph": SitemapIndexCrawlStats,
};
