// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"nabu/internal/config"
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"
)

type TestCmd struct{}

func Test(cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	exists, err := client.S3Client.Client.BucketExists(context.Background(), cfgStruct.Minio.Bucket)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("default bucket %s does not exist", cfgStruct.Minio.Bucket)
	}

	log.Info("s3 test passed")

	return err
}
