/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import styles from "./CrawlStatusDashboard.module.css";
import { stats_endpoint } from "./env";
import type { SitemapIndexCrawlStats } from "./types";

const CrawlStatusDashboard = () => {
  const [data, setData] = useState<SitemapIndexCrawlStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(stats_endpoint())
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP error ${String(res.status)}`);
        return res.json();
      })
      .then((json) => {
        setData(json as SitemapIndexCrawlStats);
        setLoading(false);
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading crawl status...</div>;
  if (error) return <div>Error loading data: {error}</div>;

  return (
    <>
      <div className={styles.headerRow}>
        <h1 className={styles.h1}>Geoconnex Crawl Status Dashboard</h1>
        <a href="/report.json" download className={styles.downloadButton}>
          Download JSON
        </a>
      </div>

      {data?.map((sitemap) => (
        <div key={sitemap.SitemapName} className={styles.sitemap}>
          <h2>Sitemap: {sitemap.SitemapName}</h2>
          <span className={styles.meta}>
            Sites Harvested: {sitemap.SitesHarvested} / {sitemap.SitesInSitemap}
            <br />
            Time to Complete: {sitemap.SecondsToComplete.toFixed(2)}s
          </span>

          <details>
            <summary className={styles.successColor}>
              Successful URLs ({sitemap.SuccessfulUrls.length})
            </summary>
            <ul className={styles.urlList}>
              {sitemap.SuccessfulUrls.map((url: string) => (
                <li key={url}>
                  <a href={url} target="_blank" rel="noopener noreferrer">
                    {url}
                  </a>
                </li>
              ))}
            </ul>
          </details>

          {(sitemap.CrawlFailures && (sitemap.CrawlFailures.length > 0)) && (
            <details>
              <summary className={styles.errorText}>
                Failures ({sitemap.CrawlFailures.length})
              </summary>
              <table className={styles.failureTable}>
                <thead>
                  <tr>
                    <th>Feature Link</th>
                    <th>Status Code</th>
                    <th>Error Message</th>
                    <th>SHACL Status</th>
                    <th>SHACL Error Message</th>
                  </tr>
                </thead>
                <tbody>
                  {sitemap.CrawlFailures.map((fail, i: number) => (
                    <tr key={i}>
                      <td>
                        <a
                          className={styles.failureLink}
                          href={fail.Url}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          Link
                        </a>
                      </td>
                      <td>{fail.Status}</td>
                      <td>{fail.Message}</td>
                      <td>{fail.ShaclStatus}</td>
                      <td>{fail.ShaclErrorMessage}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </details>
          )}
        </div>
      ))}
    </>
  );
};

export default CrawlStatusDashboard;
