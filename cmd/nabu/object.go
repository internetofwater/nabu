// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package nabu

import (
	"nabu/internal/synchronizer"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func object(objectName string) error {
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

var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "nabu object command",
	Long:  `Load a single n-quads graph object from s3 into the triplestore`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			objectVal := args[0]
			err := object(objectVal)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("must have exactly one argument which is the object to load")
		}

	},
}

func init() {
	rootCmd.AddCommand(objectCmd)
}
