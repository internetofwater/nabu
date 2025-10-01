/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import type { SitemapCrawlStats } from "./generated_types"
import vocab from "./vocab.json";

type VocabType = typeof vocab

export type SitemapCrawlStatsWithS3Metadata = SitemapCrawlStats & {
	// the last time the metadata about the sitemap 
	// was modified in S3
	LastModified: string
}

export type SitemapCrawlStatsAsJsonld = SitemapCrawlStats | SitemapCrawlStatsWithS3Metadata & {
	"@id": string
}

export interface JsonLdReport extends VocabType {
  "@graph": SitemapCrawlStatsAsJsonld[],
};