// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"

	"github.com/internetofwater/nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"
)

func Test(ctx context.Context, client *synchronizer.SynchronizerClient) error {
	exists, err := client.S3Client.Client.BucketExists(ctx, client.S3Client.DefaultBucket)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("default bucket %s does not exist", client.S3Client.DefaultBucket)
	}

	log.Info("s3 test passed")

	return nil
}
