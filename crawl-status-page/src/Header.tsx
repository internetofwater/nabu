/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import Logo from "../src/assets/geoconnex-logo.png";
import styles from "./CrawlStatusDashboard.module.css";

interface HeaderProps {
  jsonData: object;
  jsonldData?: object | null;
}

export default function Header({ jsonData, jsonldData }: HeaderProps) {
  const downloadBlob = (data: object) =>
    URL.createObjectURL(
      new Blob([JSON.stringify(data, null, 2)], {
        type: "application/json",
      })
    );

  return (
    <div className={styles.headerRow}>
      <a href="https://docs.geoconnex.us">
        <img
          src={Logo}
          alt="Internet of Water Logo"
          style={{
            scale: "0.6",
            filter: "drop-shadow(0 0 3px white)",
          }}
        />
      </a>
      <h1 className={styles.h1}>Geoconnex Crawl Status Dashboard</h1>
      <div className={styles.downloadButtonsRow}>
        {/* JSON Export */}
        <a
          href={downloadBlob(jsonData)}
          className={styles.downloadButton}
          target="_blank"
          rel="noopener noreferrer"
          onClick={(e) =>
            setTimeout(() => {
              URL.revokeObjectURL((e.currentTarget as HTMLAnchorElement).href);
            }, 1000)
          }
        >
          View as JSON
        </a>

        {/* JSON-LD Export */}
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
  );
}
