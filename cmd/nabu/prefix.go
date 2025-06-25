// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/synchronizer"
)

type UploadCmd struct{}

func upload(cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	return client.SyncTriplestoreGraphs(context.Background(), cfgStruct.Prefix, false)
}
