/**
 * Copyright 2025 Lincoln Institute of Land Policy
 * SPDX-License-Identifier: Apache-2.0
 */

import { S3 } from "@aws-sdk/client-s3";

export function use_local_services() {
  return import.meta.env.VITE_USE_GCP_FOR_DEV !== "true";
}

export function get_bucket() {
  if (import.meta.env.VITE_LOCAL_BUCKET_NAME) {
    return String(import.meta.env.VITE_LOCAL_BUCKET_NAME);
  }
  return "metadata-geoconnex-us";
}

export function get_prefix() {
  return "metadata/sitemaps";
}

export function get_minio_client() {
  return new S3({
    endpoint: "http://localhost:9000",
    region: "us-east-1",
    credentials: {
      accessKeyId: "minioadmin",
      secretAccessKey: "minioadmin",
    },
    forcePathStyle: true,
  });
}
