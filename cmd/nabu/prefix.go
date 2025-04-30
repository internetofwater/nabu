// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package nabu

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func prefix() error {
	client, err := synchronizer.NewSynchronizerClientFromConfig(cfgStruct)
	if err != nil {
		return err
	}

	for _, prefix := range cfgStruct.Prefixes {
		// sync without removal is the same as copying an entire prefix
		err = client.SyncTriplestoreGraphs(prefix, false)
		if err != nil {
			log.Error(err)
		}
	}
	return err
}

var PrefixCmd = &cobra.Command{
	Use:   "prefix",
	Short: "nabu prefix command",
	Long:  `Load graphs in s3 with a specific prefix into the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		err := prefix()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(PrefixCmd)
}
