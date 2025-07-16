/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */


import type { JsonLdReport, SitemapCrawlStats, SitemapIndexCrawlStats } from "./types";
import vocab from "./vocab.json"

export function make_jsonld(data: SitemapIndexCrawlStats) {
  const newObj = vocab as JsonLdReport;

  const graphItems: SitemapCrawlStats[] = []
  for (const graph of data) {
    const graphItem = {
      "@id": `http://geoconnex.us/sitemap/${graph.SitemapName}`,
      ...graph,
    }
    graphItems.push(graphItem);
  }
  
  newObj["@graph"] = graphItems;

  return newObj;
}
