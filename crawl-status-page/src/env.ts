/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { S3} from "@aws-sdk/client-s3";


function isDev() {
  let isDev = false;
  try {
    isDev = process.env.NODE_ENV === "development";
  } catch {
    isDev = false;
  }
  return isDev;
}

export function get_s3_bucket() {
  if (isDev()) {
    return "iow";
  } else {
    return "harvest-bucket";
  }
}

export function get_s3_client() {
  return new S3({
    endpoint: get_minio_endpoint(),
    region: "us-east-1",
    credentials: {
      accessKeyId: "minioadmin",
      secretAccessKey: "minioadmin",
    },
    // this is needed for compatibility with minio
    forcePathStyle: true
  });
}

// Set the stats endpoint to a local json if we are in development
export function get_minio_endpoint() {
  if (isDev()) {
    return "http://localhost:9000";
  } else {
    // TODO fill in with real s3 bucket endpoint
    return "https://pids.geoconnex.dev";
  }
}