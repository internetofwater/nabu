// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package nabu

import (
	"context"
	"fmt"
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func test() error {
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

var testCmd = &cobra.Command{
	Use:   "test",
	Long:  `Test to see if nabu can connect to s3 but don't do anything`,
	Short: "test to connect to s3",
	Run: func(cmd *cobra.Command, args []string) {
		err := test()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
