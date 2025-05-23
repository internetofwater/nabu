// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	crawl "github.com/internetofwater/nabu/internal/crawl"

	arg "github.com/alexflint/go-arg"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type GleanerArgs struct {
	Address            string `arg:"--address" default:"127.0.0.1" help:"address for s3"` // minio address to put data
	Port               int    `arg:"--port" default:"9000" help:"port for s3"`
	Bucket             string `arg:"--bucket" default:"iow" help:"default bucket for s3"`            // minio bucket to put data
	SitemapIndex       string `arg:"--sitemap-index" help:"sitemap index to crawl"`                  // sitemap index to crawl
	Source             string `arg:"--source" help:"source to crawl from the sitemap"`               // source to crawl from the config
	Config             string `arg:"--cfg" help:"path to config file"`                               // full path to config
	SecretKey          string `arg:"--s3-secret-key,env:S3_SECRET_KEY" default:"minioadmin"`         // secret key for minio
	AccessKey          string `arg:"--s3-access-key,env:S3_ACCESS_KEY" default:"minioadmin"`         // access key for minio
	SSL                bool   `arg:"--ssl"`                                                          // use SSL for HTTP requests
	IgnoreRobots       bool   `arg:"--ignore-robots" help:"ignore robots.txt"`                       // ignore robots.txt
	ToDisk             bool   `arg:"--to-disk" default:"false" help:"save to disk instead of minio"` // save to disk instead of minio
	LogLevel           string `arg:"--log-level" default:"INFO"`
	UseOtel            bool   `arg:"--use-otel"`
	OtelEndpoint       string `arg:"--otel-endpoint" help:"OpenTelemetry endpoint"`
	ConcurrentSitemaps int    `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int    `arg:"--sitemap-workers" default:"10"`
	HeadlessChromeUrl  string `arg:"--headless-chrome-url" default:"0.0.0.0:9222" help:"port for interacting with the headless chrome devtools"`
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

func (g GleanerRunner) Run(ctx context.Context) ([]crawl.SitemapCrawlStats, error) {
	level, err := log.ParseLevel(g.args.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", g.args.LogLevel, err)
	}
	log.SetLevel(level)
	log.Infof("Starting Gleaner with log level: %s", g.args.LogLevel)

	if g.args.UseOtel || g.args.OtelEndpoint != "" {
		if g.args.OtelEndpoint == "" {
			g.args.OtelEndpoint = opentelemetry.DefaultTracingEndpoint
		}
		log.Infof("Starting opentelemetry traces and exporting to: %s", g.args.OtelEndpoint)
		opentelemetry.InitTracer("gleaner", g.args.OtelEndpoint)
		var span trace.Span
		ctx, span = opentelemetry.SubSpanFromCtx(ctx)
		defer opentelemetry.Shutdown()
		defer span.End()
	}

	if g.args.SitemapIndex == "" {
		return nil, fmt.Errorf("sitemap index must be provided")
	}
	index, err := crawl.NewSitemapIndexHarvester(g.args.SitemapIndex)
	if err != nil {
		return nil, err
	}
	var storageDestination storage.CrawlStorage
	if g.args.ToDisk {
		log.Info("Saving fetched files to disk")
		tmpFSStorage, err := storage.NewLocalTempFSCrawlStorage()
		if err != nil {
			return nil, err
		}
		storageDestination = tmpFSStorage
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
			return nil, err
		}
		if err := minioS3.MakeDefaultBucket(); err != nil {
			return nil, err
		}
		storageDestination = minioS3
	}

	stats, err := index.
		WithStorageDestination(storageDestination).
		WithConcurrencyConfig(g.args.ConcurrentSitemaps, g.args.SitemapWorkers).
		WithSpecifiedSourceFilter(g.args.Source).
		WithHeadlessChromeUrl(g.args.HeadlessChromeUrl).
		HarvestSitemaps(ctx)

	asJson := crawl.ToJson(stats)
	if err := storageDestination.Store(fmt.Sprintf("stats/crawl_stats_%s.json", g.args.Source), strings.NewReader(asJson)); err != nil {
		return nil, err
	}

	return stats, err
}

func main() {
	if _, err := NewGleanerRunner(os.Args[1:]).Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
