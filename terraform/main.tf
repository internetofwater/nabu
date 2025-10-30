# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

# A terraform file to deploy the shacl validation server

provider "google" {
  project = "geoconnex-us"
  region  = "us"
}

resource "google_cloud_run_v2_service" "shacl_service" {
  name = "shacl-validation-grpc-server"
  # same region as the other services
  location = "us-central1"
  # allow this service to be deleted and redeployed if needed
  deletion_protection = false
  # allow unauthenticated traffic
  ingress = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      image = "docker.io/internetofwater/shacl_validator_grpc_py:latest"
      ports {
        container_port = 50051
      }
      # start the shacl validation server  
      args = ["serve"]
      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
      }
    }
  }
  # allow scale to 0 since we presume there will be times with no validation traffic
  scaling {
    max_instance_count = 1
    min_instance_count = 0
    scaling_mode       = "AUTOMATIC"
  }

  traffic {
    # all traffic should go to the latest version
    percent = 100
  }
}
