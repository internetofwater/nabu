// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"nabu/internal/config"
	"nabu/internal/synchronizer"
)



func object(objectName string, cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}
	err = client.UploadNqFileToTriplestore(objectName)
	if err != nil {
		return err
	}

	return nil
}
