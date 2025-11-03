# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

variable "project" {
  description = "The Google Cloud project ID"
  type        = string
  default     = "geoconnex-us"
}

variable "region" {
  description = "The Google Cloud region"
  type        = string
  default     = "us-central1"
}
