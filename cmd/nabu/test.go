// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"

	"github.com/internetofwater/nabu/internal/synchronizer"
	log "github.com/sirupsen/logrus"
)

// Ensure that an S3 bucket is compatible with what we need for Nabu
func Test(ctx context.Context, client *synchronizer.SynchronizerClient) error {
	exists, err := client.S3Client.Client.BucketExists(ctx, client.S3Client.DefaultBucket)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("default bucket %s does not exist", client.S3Client.DefaultBucket)
	}

	testData := []byte("test data")
	if err := client.S3Client.StoreWithHash("test", bytes.NewReader(testData), len(testData)); err != nil {
		return err
	}

	md5Hash, err := client.S3Client.GetHash("test")
	if err != nil {
		return fmt.Errorf("failed to get hash: %w", err)
	}

	calculatedHash := md5.Sum(testData)
	if md5Hash != fmt.Sprintf("%x", calculatedHash) {
		return fmt.Errorf("hashes do not match")
	}

	log.Info("S3 test passed; you should be able to use this bucket with Nabu")

	return nil
}
