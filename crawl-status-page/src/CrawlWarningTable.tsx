/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import styles from "./CrawlStatusDashboard.module.css";
import type { WarningReport } from "./generated_types";

export default function CrawlWarningTable(warnings: WarningReport) {
  return (
    <details style={{marginTop: "8px"}}>
      <summary className={styles.warningText}>
        Semantic Warnings ({warnings.TotalShaclFailures})
      </summary>
      <i className={styles.brevityInfo}>
        Displaying the first {warnings.ShaclWarnings.length} out of{" "}
        {warnings.TotalShaclFailures} total warnings for the sake of brevity
      </i>

      <table className={styles.failureTable}>
        <thead>
          <tr>
            <th>Feature Link</th>
            <th>Shacl Status</th>
            <th>Shacl Error Message</th>
          </tr>
        </thead>
        <tbody>
          {warnings.ShaclWarnings.map((shaclInfo, i: number) => (
            <tr key={i}>
              <td>
                <a
                  className={styles.warningLink}
                  href={shaclInfo.Url}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Link
                </a>
              </td>
              {/* golang uses 0 for the default value, it should be ignored since 0 isnt a valid http status */}
              <td>{shaclInfo.ShaclStatus}</td>
              <td>{shaclInfo.ShaclValidationMessage}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}
