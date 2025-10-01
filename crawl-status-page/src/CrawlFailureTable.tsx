/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */


import styles from "./CrawlStatusDashboard.module.css";
import type {  UrlCrawlError } from "./generated_types";

export default function CrawlFailureTable(failures: UrlCrawlError[]) {
  return (
    <details>
      <summary className={styles.errorText}>
        Failures ({failures.length})
      </summary>
      <table className={styles.failureTable}>
        <thead>
          <tr>
            <th>Feature Link</th>
            <th>Status Code</th>
            <th>Error Message</th>
          </tr>
        </thead>
        <tbody>
          {failures.map((fail, i: number) => (
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
              {/* golang uses 0 for the default value, it should be ignored since 0 isnt a valid http status */}
              <td>{fail.Status === 0 ? "" : fail.Status}</td>
              <td>{fail.Message}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}