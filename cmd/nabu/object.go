// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/synchronizer"
)

type ObjectCmd struct {
	Object string `arg:"positional"`
}

func object(objectName string, cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	return client.UploadNqFileToTriplestore(objectName)
}
