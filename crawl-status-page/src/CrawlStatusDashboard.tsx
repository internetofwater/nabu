/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import {
  use_local_services,
  get_bucket,
  get_prefix,
  get_minio_client,
} from "./env";
import styles from "./CrawlStatusDashboard.module.css";
import { make_jsonld } from "./lib";
import CrawlFailureTable from "./CrawlFailureTable";
import type { SitemapCrawlStats, SitemapIndexCrawlStats } from "./generated_types";
import type { SitemapCrawlStatsWithS3Metadata } from "./types";

const BUCKET = get_bucket();
const PREFIX = get_prefix();

const CrawlStatusDashboard = () => {
  const [data, setData] = useState<SitemapIndexCrawlStats>([]);
  const [jsonldData, setJsonldData] = useState<object | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;

    const fetchData = async () => {
      try {
        const sitemapcrawlstats: SitemapCrawlStats[] = [];

        if (use_local_services()) {
          // Local dev: use MinIO via S3 client
          const client = get_minio_client();
          const response = await client.listObjects({
            Bucket: BUCKET,
            Prefix: PREFIX,
          });

          for (const obj of response.Contents ?? []) {
            if (!isMounted) return;
            if (obj.Key?.endsWith(".json")) {
              try {
                const objectData = await client.getObject({
                  Bucket: BUCKET,
                  Key: obj.Key,
                });
                const body = await objectData.Body?.transformToString();
                if (!body) continue;
                const json = JSON.parse(body) as SitemapCrawlStatsWithS3Metadata;
                json.LastModified =
                  objectData.LastModified?.toISOString() ?? "Unknown";
                sitemapcrawlstats.push(json);
              } catch (e) {
                console.warn(`Error loading ${obj.Key}:`, e);
              }
            }
          }
        } else {
          // Prod: GCS JSON API
          // We have to use the JSON API since GCP doesn't allow for public buckets
          // which also have the S3 API 
          const listUrl = `https://storage.googleapis.com/storage/v1/b/${BUCKET}/o?prefix=${PREFIX}`;
          const listRes = await fetch(listUrl);
          if (!listRes.ok)
            throw new Error(`Failed to list objects: ${String(listRes.status)}`);
          const listJson = await listRes.json() as {
            items: { name: string; updated: string }[];
          };
          for (const obj of listJson.items ?? []) {
            if (!isMounted) return;
            if (obj.name.endsWith(".json")) {
              try {
                const objectUrl = `https://storage.googleapis.com/${BUCKET}/${obj.name}`;
                const objectRes = await fetch(objectUrl);
                if (!objectRes.ok)
                  throw new Error(`Failed to fetch ${obj.name}`);
                const json = await objectRes.json() as SitemapCrawlStatsWithS3Metadata;
                json.LastModified = obj.updated ?? "Unknown";
                sitemapcrawlstats.push(json);
              } catch (e) {
                console.warn(`Error loading ${obj.name}:`, e);
              }
            }
          }
        }

        if (isMounted) {
          setData(sitemapcrawlstats);
          const jsonld = make_jsonld(sitemapcrawlstats);
          setJsonldData(jsonld);
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


  return (
    <>
      {error ? (
        <p style={{ color: "var(--error-bg)", textAlign: "center" }}>
          Error loading report: <i> {error} </i>
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
                CrawlFailureTable(sitemap.CrawlFailures)
            )}
          </div>
        ))
      )}
    </>
  );
};

export default CrawlStatusDashboard;
