/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */


// Set the stats endpoint to a local json if we are in development
export function stats_endpoint() {
    let isDev = false
    try {
        isDev = process.env.NODE_ENV === "development";
    } catch {
        isDev = false
    }

    if (isDev) {
        return "/example_crawl_stats.json"
    } else {
        // TODO fill in with real s3 bucket endpoint
        return "https://api.geoconnex.dev"
    }
}