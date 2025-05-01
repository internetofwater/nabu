// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"nabu/internal/config"
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"
)

func Sync(cfgStruct config.NabuConfig) error {
	log.Info("dropping graphs in triplestore not in s3 and adding graphs to triplestore that are missing from it but present in s3")
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	for _, prefix := range cfgStruct.Prefixes {
		err = client.SyncTriplestoreGraphs(context.Background(), prefix, true)
		if err != nil {
			return err
		}
	}
	return err
}
