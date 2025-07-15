/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import CrawlStatusDashboard from "./App.tsx";

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <CrawlStatusDashboard />
  </StrictMode>
);
