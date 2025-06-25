// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"

	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"
)

type SyncCmd struct{}

func Sync(ctx context.Context, cfgStruct config.NabuConfig) error {
	log.Info("dropping graphs in triplestore not in s3 and adding graphs to triplestore that are missing from it but present in s3")
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	return client.SyncTriplestoreGraphs(ctx, cfgStruct.Prefix, true)
}
