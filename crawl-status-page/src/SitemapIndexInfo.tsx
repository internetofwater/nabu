/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import styles from "./CrawlStatusDashboard.module.css";

export default function SitemapIndexInfo({
  sitemapIndex,
  sitemapsCurrentlyShown,
}: {
  sitemapIndex: string;
  sitemapsCurrentlyShown: number;
}) {
  const [sitemapsInIndex, setSitemapsInIndex] = useState<number | null>(null);

  useEffect(() => {
    if (!sitemapIndex) return;

    const controller = new AbortController();

    const fetchSitemap = async () => {
      try {
        const response = await fetch(sitemapIndex, {
          signal: controller.signal,
        });
        if (!response.ok) throw new Error(`HTTP error: ${response.status.toString()}`);
        const text = await response.text();

        const parser = new DOMParser();
        const xml = parser.parseFromString(text, "application/xml");
        const sitemaps = xml.getElementsByTagName("sitemap");
        setSitemapsInIndex(sitemaps.length);
      } catch (error) {
        if ((error as Error).name !== "AbortError") {
          console.error("Failed to fetch sitemap:", error);
          setSitemapsInIndex(null);
        }
      }
    };

    void fetchSitemap();

    return () => {
      controller.abort();
    };
  }, [sitemapIndex]);

  return (
    <div className={styles.brevityInfo} style={{marginBottom: "16px"}}>
      {sitemapsInIndex !== null && (
        <div>
          <strong>Showing reports for {sitemapsCurrentlyShown} sitemaps out of <a href={sitemapIndex} target="_blank" rel="noopener noreferrer">{sitemapsInIndex} in total </a> </strong>
        </div>
      )}
    </div>
  );
}
