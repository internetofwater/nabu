// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/synchronizer"
)

type ClearCmd struct{}

func clear(cfgStruct config.NabuConfig) error {
	synchronizerClient, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	return synchronizerClient.GraphClient.ClearAllGraphs()
}
