// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"nabu/internal/config"
	"nabu/internal/interfaces"
	"nabu/internal/opentelemetry"
	"nabu/internal/synchronizer/s3"
	"os"

	crawl "nabu/internal/crawl"

	arg "github.com/alexflint/go-arg"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type GleanerArgs struct {
	Address      string `arg:"--address" default:"127.0.0.1" help:"address for s3"` // minio address to put data
	Port         int    `arg:"--port" default:"9000" help:"port for s3"`
	Bucket       string `arg:"--bucket" default:"iow" help:"default bucket for s3"` // minio bucket to put data
	SitemapIndex string `arg:"--sitemap-index" help:"sitemap index to crawl"`       // sitemap index to crawl
	Source       string `arg:"--source" help:"source to crawl from the sitemap"`    // source to crawl from the config
	Config       string `arg:"--cfg" help:"path to config file"`                    // full path to config
	// Mode         string `arg:"--mode"`
	SecretKey    string `arg:"--secret-key" default:"minioadmin"`                              // secret key for minio
	AccessKey    string `arg:"--access-key" default:"minioadmin"`                              // access key for minio
	SSL          bool   `arg:"--ssl"`                                                          // use SSL for HTTP requests
	IgnoreRobots bool   `arg:"--ignore-robots" help:"ignore robots.txt"`                       // ignore robots.txt
	ToDisk       bool   `arg:"--to-disk" default:"false" help:"save to disk instead of minio"` // save to disk instead of minio
	LogLevel     string `arg:"--log-level" default:"INFO"`
	UseOtel      bool   `arg:"--use-otel"`
	OtelEndpoint string `arg:"--otel-endpoint" help:"OpenTelemetry endpoint"`

	ConcurrentSitemaps int `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int `arg:"--sitemap-workers" default:"10"`
}

type GleanerRunner struct {
	args GleanerArgs
}

func NewGleanerRunner(cliArgs []string) GleanerRunner {
	args := GleanerArgs{}
	const dummyBinaryName = "gleaner" // we need to add some arbitrary binary name; it doesn't matter
	os.Args = append([]string{dummyBinaryName}, cliArgs...)
	arg.MustParse(&args)
	return GleanerRunner{
		args: args,
	}
}

func (g GleanerRunner) Run(ctx context.Context) error {
	level, err := log.ParseLevel(g.args.LogLevel)
	if err != nil {
		return fmt.Errorf("invalid log level %s: %w", g.args.LogLevel, err)
	}
	log.SetLevel(level)
	log.SetOutput(os.Stdout)
	log.Info("Starting Gleaner")
	log.Debug("Running in debug mode")

	if g.args.UseOtel || g.args.OtelEndpoint != "" {
		if g.args.OtelEndpoint == "" {
			g.args.OtelEndpoint = opentelemetry.DefaultCollectorEndpoint
		}
		log.Infof("Starting opentelemetry traces and exporting to: %s", g.args.OtelEndpoint)
		opentelemetry.InitTracer("gleaner", g.args.OtelEndpoint)
		var span trace.Span
		span, ctx = opentelemetry.SubSpanFromCtx(ctx)
		defer span.End()
		defer opentelemetry.Shutdown()
	}

	if g.args.SitemapIndex != "" {
		index, err := crawl.NewSitemapIndexHarvester(g.args.SitemapIndex)
		if err != nil {
			return err
		}
		var configuredSitemap crawl.Index
		if g.args.ToDisk {
			log.Info("Saving fetched files to disk")
			tmpFSStorage, err := interfaces.NewLocalTempFSCrawlStorage()
			if err != nil {
				return err
			}
			configuredSitemap = index.WithStorageDestination(tmpFSStorage)
		} else {
			log.Infof("Saving fetched files to s3 bucket at %s:%d", g.args.Address, g.args.Port)
			minioS3, err := s3.NewMinioClientWrapper(config.MinioConfig{
				Address:   g.args.Address,
				Port:      g.args.Port,
				Ssl:       g.args.SSL,
				Accesskey: g.args.AccessKey,
				Secretkey: g.args.SecretKey,
				Bucket:    g.args.Bucket,
			})
			if err != nil {
				return err
			}
			if err := minioS3.MakeDefaultBucket(); err != nil {
				return err
			}

			configuredSitemap = index.WithStorageDestination(minioS3)
		}

		configuredSitemap = configuredSitemap.WithConcurrencyConfig(g.args.ConcurrentSitemaps, g.args.SitemapWorkers)

		if g.args.Source != "" {
			return configuredSitemap.HarvestSitemap(ctx, g.args.Source)
		} else {
			return configuredSitemap.HarvestAll(ctx)
		}
	} else if g.args.Source != "" {
		panic("not implemented")
	}

	return nil
}

func main() {
	if err := NewGleanerRunner(os.Args[1:]).Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
