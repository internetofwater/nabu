/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import { get_bucket, get_prefix, get_minio_client, use_gcp } from "./env";
import styles from "./CrawlStatusDashboard.module.css";
import { make_jsonld } from "./lib";
import CrawlFailureTable from "./CrawlFailureTable";
import type { SitemapCrawlStatsWithS3Metadata } from "./types";
import Header from "./Header";
import CrawlWarningTable from "./CrawlWarningTable";
import SitemapIndexInfo from "./SitemapIndexInfo";

const BUCKET = get_bucket();
const PREFIX = get_prefix();

interface SitemapItemState {
  key: string;
  loading: boolean;
  error?: string;
  data?: SitemapCrawlStatsWithS3Metadata;
}

const CrawlStatusDashboard = () => {
  const [sitemaps, setSitemaps] = useState<SitemapItemState[]>([]);
  const [jsonldData, setJsonldData] = useState<object | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Whenever we get fully loaded sitemap data, update jsonld
  useEffect(() => {
    const loaded = sitemaps
      .map((s) => s.data)
      .filter((d): d is SitemapCrawlStatsWithS3Metadata => !!d);

    if (loaded.length > 0) {
      setJsonldData(make_jsonld(loaded));
    }
  }, [sitemaps]);

  useEffect(() => {
    let isMounted = true;

    const loadSitemaps = async () => {
      try {
        if (!use_gcp()) {
          const client = get_minio_client();
          const response = await client.listObjects({
            Bucket: BUCKET,
            Prefix: PREFIX,
          });

          const objects = response.Contents ?? [];
          if (!isMounted) return;

          // Initialize state placeholders
          setSitemaps(
            objects
              .filter((o) => o.Key?.endsWith(".json"))
              .map((o) => ({ key: o.Key ?? "", loading: true }))
          );

          // Load each async
          for (const obj of objects) {
            if (!obj.Key?.endsWith(".json") || !isMounted) continue;

            try {
              const objectData = await client.getObject({
                Bucket: BUCKET,
                Key: obj.Key,
              });
              const body = await objectData.Body?.transformToString();
              if (!body) throw new Error("Empty body");

              const json = JSON.parse(body) as SitemapCrawlStatsWithS3Metadata;
              json.LastModified =
                objectData.LastModified?.toISOString() ?? "Unknown";

              if (isMounted) {
                setSitemaps((prev) =>
                  prev.map((s) =>
                    s.key === obj.Key ? { ...s, loading: false, data: json } : s
                  )
                );
              }
            } catch (e) {
              if (isMounted) {
                setSitemaps((prev) =>
                  prev.map((s) =>
                    s.key === obj.Key
                      ? {
                          ...s,
                          loading: false,
                          error: `Error loading ${obj.Key}`,
                        }
                      : s
                  )
                );
              }
              console.warn(`Error loading ${obj.Key}:`, e);
            }
          }
        } else {
          // GCS (production)
          const listUrl = `https://storage.googleapis.com/storage/v1/b/${BUCKET}/o?prefix=${PREFIX}`;
          const listRes = await fetch(listUrl);
          if (!listRes.ok)
            throw new Error(
              `Failed to list objects: ${String(listRes.status)}`
            );

          const listJson = (await listRes.json()) as {
            items: { name: string; updated: string }[];
          };

          if (!isMounted) return;

          setSitemaps(
            listJson.items
              .filter((o) => o.name.endsWith(".json"))
              .map((o) => ({ key: o.name, loading: true }))
          );

          for (const obj of listJson.items) {
            if (!obj.name.endsWith(".json") || !isMounted) continue;

            try {
              const objectUrl = `https://storage.googleapis.com/${BUCKET}/${obj.name}`;
              const objectRes = await fetch(objectUrl);
              if (!objectRes.ok) throw new Error(`Failed to fetch ${obj.name}`);

              const json =
                (await objectRes.json()) as SitemapCrawlStatsWithS3Metadata;
              json.LastModified = obj.updated ?? "Unknown";

              if (isMounted) {
                setSitemaps((prev) =>
                  prev.map((s) =>
                    s.key === obj.name
                      ? { ...s, loading: false, data: json }
                      : s
                  )
                );
              }
            } catch (e) {
              if (isMounted) {
                setSitemaps((prev) =>
                  prev.map((s) =>
                    s.key === obj.name
                      ? {
                          ...s,
                          loading: false,
                          error: `Error loading ${obj.name}`,
                        }
                      : s
                  )
                );
              }
              console.warn(`Error loading ${obj.name}:`, e);
            }
          }
        }
      } catch (err: unknown) {
        if (isMounted) {
          setError(err instanceof Error ? err.message : String(err));
          console.error(
            `Error loading sitemaps from ${BUCKET} with prefix ${PREFIX}:`,
            err
          );
        }
      }
    };

    void loadSitemaps();
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <div className={styles.dashboardContainer}>
      <Header
        jsonData={
          sitemaps
            .map((s) => s.data)
            .filter(Boolean) as SitemapCrawlStatsWithS3Metadata[]
        }
        jsonldData={jsonldData}
      />

      {error && (
        <p style={{ color: "var(--error-bg)", textAlign: "center" }}>
          Error loading report: <i>{error}</i>
        </p>
      )}
      <SitemapIndexInfo
        sitemapsCurrentlyShown={sitemaps.length}
        sitemapIndex={"https://geoconnex.us/sitemap.xml"}
      />
      {sitemaps.map((s) => (
        <div key={s.key} className={styles.sitemap}>
          {s.loading && <p>Loading sitemap {s.key}…</p>}
          {s.error && (
            <p style={{ color: "var(--error-bg)" }}>
              Failed to load {s.key}: {s.error}
            </p>
          )}
          {s.data && (
            <>
              <div className={styles.sitemapHeaderRow}>
                <h2>
                  Sitemap:{" "}
                  {s.data.SitemapSourceLink ? (
                    <a
                      href={s.data.SitemapSourceLink}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {s.data.SitemapName}
                    </a>
                  ) : (
                    s.data.SitemapName
                  )}
                </h2>
                <span style={{ color: "gray" }}>
                  Last Modified: {s.data.LastModified?.split("T")[0]}
                </span>
              </div>
              <span className={styles.meta}>
                Sites in Sitemap: {s.data.SitesInSitemap}
                <br />
                Time to Complete: {s.data.SecondsToComplete.toFixed(2)}s
              </span>
              <strong>
                <p className={styles.successColor}>
                  Successful Features: {s.data.SuccessfulSites}
                </p>
              </strong>

              {s.data.WarningStats &&
                s.data.WarningStats.TotalShaclFailures > 0 &&
                CrawlWarningTable(s.data.WarningStats)}

              {s.data.CrawlFailures &&
                s.data.CrawlFailures.length > 0 &&
                CrawlFailureTable(s.data.CrawlFailures)}
            </>
          )}
        </div>
      ))}
    </div>
  );
};

export default CrawlStatusDashboard;
