/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import styles from "./CrawlStatusDashboard.module.css";
import type { SitemapCrawlStats, SitemapIndexCrawlStats } from "./types";
import { get_s3_bucket, get_s3_client, get_minio_endpoint } from "./env";
import { make_jsonld } from "./lib";

const CrawlStatusDashboard = () => {
  const [data, setData] = useState<SitemapIndexCrawlStats>([]);
  const [jsonldData, setJsonldData] = useState<object | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const client = get_s3_client();

  useEffect(() => {
    let isMounted = true;
    const fetchData = async () => {
      try {
        const response = await client.listObjects({
          Bucket: get_s3_bucket(),
          Prefix: "metadata/sitemaps",
        });

        const sitemapcrawlstats: SitemapCrawlStats[] = [];

        for (const obj of response.Contents ?? []) {
          if (!isMounted) return;
          if (obj.Key?.endsWith(".json")) {
            try {
              const objectData = await client.getObject({
                Bucket: get_s3_bucket(),
                Key: obj.Key,
              });

              const lastModified = objectData.LastModified;
              const body = await objectData.Body?.transformToString();
              if (!body) {
                if (!isMounted) return;
                setError(`No body for object ${obj.Key}`);
                return;
              }

              const json = JSON.parse(body) as SitemapCrawlStats;
              json.LastModified = lastModified
                ? lastModified.toISOString()
                : "Unknown";
              sitemapcrawlstats.push(json);
            } catch (e) {
              console.warn(`Error loading ${obj.Key}:`, e);
            }
          }
        }

        if (isMounted) {
          setData(sitemapcrawlstats);
          const jsonld = make_jsonld(sitemapcrawlstats);
          if (isMounted) setJsonldData(jsonld);
          setLoading(false);
        }
      } catch (err: unknown) {
        if (isMounted) {
          setError(err instanceof Error ? err.message : String(err));
          console.error(err);
          setLoading(false);
        }
      }
    };

    void fetchData();
    return () => {
      isMounted = false;
    };
  }, []);

  if (loading) return <div>Loading crawl status...</div>;

  const downloadBlob = (data: object) =>
    URL.createObjectURL(
      new Blob([JSON.stringify(data, null, 2)], {
        type: "application/json",
      })
    );

  return (
    <>
      <div className={styles.headerRow}>
        <a href="https://docs.geoconnex.us">
          <img
            src="/src/assets/geoconnex-logo.png"
            style={{
              scale: "0.6",
              filter: "drop-shadow(0 0 3px white)",
            }}
          />
        </a>
        <h1 className={styles.h1}>Geoconnex Crawl Status Dashboard</h1>
        <div className={styles.downloadButtonsRow}>
          <a
            href={downloadBlob(data)}
            className={styles.downloadButton}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) =>
              setTimeout(() => {
                URL.revokeObjectURL(
                  (e.currentTarget as HTMLAnchorElement).href
                );
              }, 1000)
            }
          >
            View as JSON
          </a>

          {jsonldData && (
            <a
              href={downloadBlob(jsonldData)}
              className={styles.downloadButton}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) =>
                setTimeout(() => {
                  URL.revokeObjectURL(
                    (e.currentTarget as HTMLAnchorElement).href
                  );
                }, 1000)
              }
            >
              View as JSON-LD
            </a>
          )}
        </div>
      </div>
      {error ? (
        <p style={{ color: "var(--error-bg)", textAlign: "center" }}>
          Error loading report from {get_minio_endpoint()}: <i> {error} </i>
        </p>
      ) : (
        data.map((sitemap) => (
          <div key={sitemap.SitemapName} className={styles.sitemap}>
            <div className={styles.sitemapHeaderRow}>
              <h2>Sitemap: {sitemap.SitemapName}</h2>
              <span style={{ color: "gray" }}>
                Last Modified: {sitemap.LastModified?.split("T")[0]}
              </span>
            </div>
            <span className={styles.meta}>
              Sites Harvested: {sitemap.SitesHarvested} /{" "}
              {sitemap.SitesInSitemap}
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

            {sitemap.CrawlFailures && sitemap.CrawlFailures.length > 0 && (
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
        ))
      )}
    </>
  );
};

export default CrawlStatusDashboard;
