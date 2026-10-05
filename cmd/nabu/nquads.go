// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"

	"github.com/internetofwater/nabu/internal/synchronizer"
)

// Command to convert the JSON-LD documents in harvested parquet files to N-Quads
type NquadsCmd struct {
	Files []string `arg:"positional" help:"local parquet files to convert; if none are given, every parquet file under --prefix (default summoned/) in the s3 bucket is converted"`
}

// Write the N-Quads for every JSON-LD document in the parquet files to out
func Nquads(ctx context.Context, synchronizerClient *synchronizer.SynchronizerClient, args NquadsCmd, prefix string, out io.Writer) error {
	var sources []synchronizer.NquadsSource
	if len(args.Files) > 0 {
		for _, file := range args.Files {
			sources = append(sources, synchronizer.NewLocalNquadsSource(file))
		}
	} else {
		if prefix == "" {
			prefix = "summoned/"
		}
		var err error
		sources, err = synchronizerClient.NewS3NquadsSources(ctx, prefix)
		if err != nil {
			return err
		}
	}
	return synchronizerClient.WriteNquads(ctx, out, sources)
}
