// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"nabu/internal/config"
	"nabu/internal/synchronizer"
)

func prefix(cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	for _, prefix := range cfgStruct.Prefixes {
		// sync without removal is the same as copying an entire prefix
		err = client.SyncTriplestoreGraphs(context.Background(), prefix, false)
		if err != nil {
			return err
		}
	}
	return err
}
