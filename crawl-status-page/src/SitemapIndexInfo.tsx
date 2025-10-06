/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import styles from "./CrawlStatusDashboard.module.css";

export default function SitemapIndexInfo() {
  const [count, setCount] = useState<number | null>(null);
  const sitemapUrl = "https://geoconnex.us/sitemap.xml";

  useEffect(() => {
    const fetchSitemapIndex = async () => {
      try {
        const response = await fetch(sitemapUrl);
        const text = await response.text();

        const parser = new DOMParser();
        const xmlDoc = parser.parseFromString(text, "application/xml");

        const sitemaps = xmlDoc.getElementsByTagName("sitemap");
        setCount(sitemaps.length);
      } catch (error) {
        console.error("Error fetching sitemap index:", error);
        setCount(0);
      }
    };

    // ✅ Correct way — don't use `await` directly here
    void fetchSitemapIndex();
  }, [sitemapUrl]);

  return (
    <div className="info">
      <p className={styles.brevityInfo}>
        Displaying from{" "}
        <a href={sitemapUrl} target="_blank" rel="noopener noreferrer">
          {sitemapUrl}
        </a>
      </p>
      {count !== null ? (
        <p>There are {count} sitemaps in the index.</p>
      ) : (
        <p>Loading sitemap index...</p>
      )}
    </div>
  );
}
