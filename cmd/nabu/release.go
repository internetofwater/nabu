// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"nabu/internal/config"
	"nabu/internal/synchronizer"
)

type ReleaseCmd struct{}

func release(cfgStruct config.NabuConfig) error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	for _, prefix := range cfgStruct.Prefixes {
		err = client.GenerateNqRelease(prefix)
		if err != nil {
			return err
		}
	}

	return err
}
