// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"

	"github.com/internetofwater/nabu/internal/synchronizer"
	"github.com/minio/minio-go/v7"
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

	md5Hash, exists, err := client.S3Client.GetHash("test")
	if err != nil {
		return fmt.Errorf("failed to get hash: %w", err)
	}

	if !exists {
		return fmt.Errorf("the file does not exist")
	}

	calculatedHash := md5.Sum(testData)
	if md5Hash != fmt.Sprintf("%x", calculatedHash) {
		return fmt.Errorf("hashes do not match")
	}

	const snowballObjectCount = 3
	snowObjChan := make(chan minio.SnowballObject, snowballObjectCount)
	for obj := range snowballObjectCount {
		snowObjChan <- minio.SnowballObject{
			Key:     fmt.Sprintf("test_snowball_%d", obj),
			Size:    int64(len(testData)),
			Content: bytes.NewReader(testData),
		}
	}
	close(snowObjChan)
	if err := client.S3Client.Client.PutObjectsSnowball(ctx, client.S3Client.DefaultBucket, minio.SnowballOptions{}, snowObjChan); err != nil {
		return fmt.Errorf("failed to put objects with Snowball; snowball bulk uploads may not be supported: %w", err)
	}

	for obj := range snowballObjectCount {
		data, err := client.S3Client.Get(fmt.Sprintf("test_snowball_%d", obj))
		if err != nil {
			return fmt.Errorf("failed to get object: %w", err)
		}
		dataAsString, err := io.ReadAll(data)
		if err != nil {
			return fmt.Errorf("failed to read object data: %w", err)
		}
		if !bytes.Equal(dataAsString, testData) {
			return fmt.Errorf("data does not match")
		}
	}

	log.Info("Storage test passed; you should be able to use this bucket with Nabu")

	return nil
}
