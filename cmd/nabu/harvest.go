// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	crawl "github.com/internetofwater/nabu/internal/crawl"

	log "github.com/sirupsen/logrus"
)

// Command to harvest sitemaps and store them in a specified storage destination (S3 or local disk).
// This was previously known as "gleaner" and is now integrated into the nabu command line tool.
type HarvestCmd struct {
	SitemapIndex       string `arg:"--sitemap-index" help:"sitemap index to crawl"`                  // sitemap index to crawl
	Source             string `arg:"--source" help:"source to crawl from the sitemap"`               // source to crawl from the config
	IgnoreRobots       bool   `arg:"--ignore-robots" help:"ignore robots.txt"`                       // ignore robots.txt
	ToDisk             bool   `arg:"--to-disk" default:"false" help:"save to disk instead of minio"` // save to disk instead of minio
	UseOtel            bool   `arg:"--use-otel"`
	ConcurrentSitemaps int    `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int    `arg:"--sitemap-workers" default:"10"`
	HeadlessChromeUrl  string `arg:"--headless-chrome-url" default:"0.0.0.0:9222" help:"port for interacting with the headless chrome devtools"`
	ValidateShacl      bool   `arg:"--validate-shacl" default:"false" help:"validate the sitemap against Geoconnex SHACL shapes; requires the gRPC server to be running"`
}

func Harvest(ctx context.Context, minioConfig config.MinioConfig, args HarvestCmd) ([]crawl.SitemapCrawlStats, error) {
	if args.SitemapIndex == "" {
		return nil, fmt.Errorf("sitemap index must be provided")
	}
	index, err := crawl.NewSitemapIndexHarvester(args.SitemapIndex)
	if err != nil {
		return nil, err
	}
	var storageDestination storage.CrawlStorage
	if args.ToDisk {
		log.Info("Saving fetched files to disk")
		tmpFSStorage, err := storage.NewLocalTempFSCrawlStorage()
		if err != nil {
			return nil, err
		}
		storageDestination = tmpFSStorage
	} else {
		log.Infof("Saving fetched files to s3 bucket at %s:%d", minioConfig.Address, minioConfig.Port)
		minioS3, err := s3.NewMinioClientWrapper(minioConfig)
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
		WithConcurrencyConfig(args.ConcurrentSitemaps, args.SitemapWorkers).
		WithSpecifiedSourceFilter(args.Source).
		WithHeadlessChromeUrl(args.HeadlessChromeUrl).
		WithShaclValidationConfig(args.ValidateShacl).
		HarvestSitemaps(ctx)

	asJson := crawl.ToJson(stats)
	if err := storageDestination.Store(fmt.Sprintf("stats/crawl_stats_%s.json", args.Source), strings.NewReader(asJson)); err != nil {
		return nil, err
	}

	return stats, err
}
