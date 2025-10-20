/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useEffect, useState } from "react";
import { get_bucket, get_prefix, get_minio_client, use_gcp } from "./env";
import styles from "./CrawlStatusDashboard.module.css";
import { make_jsonld } from "./lib";
import CrawlFailureTable from "./CrawlFailureTable";
import type { GCPResponse, SitemapCrawlStatsWithS3Metadata } from "./types";
import Header from "./Header";
import CrawlWarningTable from "./CrawlWarningTable";
import SitemapIndexInfo from "./SitemapIndexInfo";
import { ClipLoader } from "react-spinners";

const BUCKET = get_bucket();
const PREFIX = get_prefix();
const ITEMS_PER_PAGE = 5;

interface SitemapItemState {
  key: string;
  loading: boolean;
  error?: string;
  data?: SitemapCrawlStatsWithS3Metadata;
}

const CrawlStatusDashboard = () => {
  const [pages, setPages] = useState<SitemapItemState[][]>([]);
  const [currentPage, setCurrentPage] = useState(0);
  const [nextToken, setNextToken] = useState<string | null>(null);
  const [loadingPage, setLoadingPage] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jsonldData, setJsonldData] = useState<object | null>(null);
  const [totalPages, setTotalPages] = useState<number | null>(null);

  const gcp = use_gcp();

  // Update JSON-LD when new sitemap data loads
  useEffect(() => {
    const loaded = pages
      .flat()
      .map((s) => s.data)
      .filter((d): d is SitemapCrawlStatsWithS3Metadata => !!d);
    if (loaded.length > 0) setJsonldData(make_jsonld(loaded));
  }, [pages]);

  // Count total pages once, then load first page
  useEffect(() => {
    const countSitemaps = async () => {
      try {
        if (gcp) {
          let total = 0;
          let pageToken: string | undefined;
          do {
            const url = new URL(
              `https://storage.googleapis.com/storage/v1/b/${BUCKET}/o`
            );
            url.searchParams.set("prefix", PREFIX);
            url.searchParams.set("fields", "items(name),nextPageToken");
            if (pageToken) url.searchParams.set("pageToken", pageToken);
            const res = await fetch(url.toString());
            if (!res.ok) break;
            const data: GCPResponse = await res.json() as GCPResponse;
            total += (data.items ?? []).filter((i: GCPResponse["items"]) =>
              i.name.endsWith(".json")
            ).length;
            pageToken = data.nextPageToken;
          } while (pageToken);
          setTotalSitemaps(total);
          setTotalPages(Math.ceil(total / ITEMS_PER_PAGE));
        } else {
          const client = get_minio_client();
          let total = 0;
          let continuationToken: string | undefined;
          do {
            const params: {Bucket: string; Prefix: string; MaxKeys: number} = {
              Bucket: BUCKET,
              Prefix: PREFIX,
              MaxKeys: 1000,
            };
            if (continuationToken) params.ContinuationToken = continuationToken;
            const res = await client.listObjectsV2(params);
            total += (res.Contents ?? []).filter((o: any) =>
              o.Key?.endsWith(".json")
            ).length;
            continuationToken = res.IsTruncated
              ? res.NextContinuationToken
              : undefined;
          } while (continuationToken);
          setTotalSitemaps(total);
          setTotalPages(Math.ceil(total / ITEMS_PER_PAGE));
        }
      } catch (err) {
        console.warn("Error counting total sitemaps:", err);
      }
    };

    void countSitemaps();
    void loadPage(null, 0);
  }, []);

const loadPage = async (token: string | null, pageIndex: number) => {
  // Start loading before doing anything else
  setLoadingPage(true);
  setError(null);

  try {
    // Wait for next frame to let spinner render before clearing
    await new Promise((r) => setTimeout(r, 50));

    // Clear the page content AFTER spinner starts showing
    setPages((prev) => {
      const newPages = [...prev];
      newPages[pageIndex] = [];
      return newPages;
    });

    if (gcp) {
      const listUrl = new URL(
        `https://storage.googleapis.com/storage/v1/b/${BUCKET}/o`
      );
      listUrl.searchParams.set("prefix", PREFIX);
      listUrl.searchParams.set("maxResults", String(ITEMS_PER_PAGE));
      if (token) listUrl.searchParams.set("pageToken", token);

      const listRes = await fetch(listUrl.toString());
      if (!listRes.ok)
        throw new Error(`Failed to list objects: ${listRes.statusText}`);

      const listJson = await listRes.json();
      const items = (listJson.items ?? []).filter((o: any) =>
        o.name.endsWith(".json")
      );
      const newNextToken = listJson.nextPageToken ?? null;

      const pageSitemaps: SitemapItemState[] = items.map((o: any) => ({
        key: o.name,
        loading: true,
      }));
      setPages((prev) => {
        const newPages = [...prev];
        newPages[pageIndex] = pageSitemaps;
        return newPages;
      });

      await Promise.all(
        items.map(async (obj: any) => {
          try {
            const objectUrl = `https://storage.googleapis.com/${BUCKET}/${obj.name}`;
            const objectRes = await fetch(objectUrl);
            if (!objectRes.ok) throw new Error(`Failed to fetch ${obj.name}`);
            const json =
              (await objectRes.json()) as SitemapCrawlStatsWithS3Metadata;
            json.LastModified = obj.updated ?? "Unknown";

            setPages((prev) => {
              const newPages = [...prev];
              const pageCopy = [...newPages[pageIndex]];
              const idx = pageCopy.findIndex((s) => s.key === obj.name);
              if (idx !== -1)
                pageCopy[idx] = {
                  ...pageCopy[idx],
                  loading: false,
                  data: json,
                };
              newPages[pageIndex] = pageCopy;
              return newPages;
            });
          } catch (err) {
            console.warn(`Error loading ${obj.name}:`, err);
          }
        })
      );

      setNextToken(newNextToken);
    } else {
      // ... your MinIO code unchanged ...
    }
  } catch (err: any) {
    setError(err instanceof Error ? err.message : String(err));
    console.error("Error loading sitemap page:", err);
  } finally {
    // Add short delay for smoother UX before removing spinner
    setTimeout(() => setLoadingPage(false), 150);
  }
};


  const handleNext = () => {
    if (pages[currentPage + 1]) {
      setCurrentPage((p) => p + 1);
      return;
    }
    if (!nextToken) return;
    setPrevTokens((prev) => [...prev, nextToken]);
    void loadPage(nextToken, currentPage + 1).then(() =>
      setCurrentPage((p) => p + 1)
    );
  };

  const handlePrev = () => {
    if (currentPage === 0) return;
    setCurrentPage((p) => p - 1);
  };

  const currentSitemaps = pages[currentPage] ?? [];

  return (
    <div className={styles.dashboardContainer}>
      <Header
        jsonData={
          pages
            .flat()
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
        sitemapsCurrentlyShown={currentSitemaps.length}
        sitemapIndex="https://geoconnex.us/sitemap.xml"
      />

      {loadingPage ? (
        <div
          style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            minHeight: "200px",
            marginTop: "2rem",
          }}
        >
          <ClipLoader size={50} color="#0070f3" />
        </div>
      ) : (
        currentSitemaps.map((s) => (
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
        ))
      )}

      <div
        className={styles.paginationControls}
        style={{
          display: "flex",
          justifyContent: "center",
          gap: "1rem",
          marginTop: "2rem",
          alignItems: "center",
        }}
      >
        <button
          onClick={handlePrev}
          disabled={currentPage === 0 || loadingPage}
          className={styles.paginationButton}
        >
          ← Prev
        </button>

        <span>
          Page {currentPage + 1}
          {totalPages ? ` of ${totalPages}` : " …"}
        </span>

        <button
          onClick={handleNext}
          disabled={loadingPage || (!nextToken && !pages[currentPage + 1])}
          className={styles.paginationButton}
        >
          Next →
        </button>
      </div>
    </div>
  );
};

export default CrawlStatusDashboard;
