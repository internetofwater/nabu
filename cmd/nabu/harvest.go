// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"
	"github.com/internetofwater/nabu/pkg"

	crawl "github.com/internetofwater/nabu/internal/crawl"

	log "github.com/sirupsen/logrus"
)

// Command to harvest sitemaps and store them in a specified storage destination (S3 or local disk).
// This was previously known as "gleaner" and is now integrated into the nabu command line tool.
type HarvestCmd struct {
	Source             string `arg:"--source" help:"source to crawl from the sitemap"`               // source to crawl from the config
	IgnoreRobots       bool   `arg:"--ignore-robots" help:"ignore robots.txt"`                       // ignore robots.txt
	ToDisk             bool   `arg:"--to-disk" default:"false" help:"save to disk instead of minio"` // save to disk instead of minio
	UseOtel            bool   `arg:"--use-otel"`
	ConcurrentSitemaps int    `arg:"--concurrent-sitemaps" default:"10"`
	SitemapWorkers     int    `arg:"--sitemap-workers" default:"10"`
	HeadlessChromeUrl  string `arg:"--headless-chrome-url" default:"0.0.0.0:9222" help:"port for interacting with the headless chrome devtools"`
	ShaclEndpoint      string `arg:"--shacl-grpc-endpoint" default:"" help:"full shacl grpc endpoint with port to use for validation; if empty and --shacl-local is not set, skip validation"`
	ShaclLocal         bool   `arg:"--shacl-local" default:"false" help:"validate jsonld against the bundled Geoconnex SHACL shape in process instead of with a grpc server"`
	ExitOnShaclFailure bool   `arg:"--exit-on-shacl-failure" default:"false" help:"immediately exit if shacl validation fails"`
	MainstemFile       string `arg:"--mainstem-metadata" help:"path to a flatgeobuf mainstem file, either local or in s3/gcs/http, used to put the mainstem uri of each feature in the mainstem_uri column; required if a harvested sitemap requests mainstem associations in the sitemap index"`
}

// Make sure a local mainstem file exists so a typo fails before any sitemap is harvested
func validateMainstemFile(mainstemFile string) error {
	if mainstemFile == "" {
		return nil
	}
	for _, remotePrefix := range []string{"gcs://", "gs://", "s3://", "http://", "https://"} {
		if strings.HasPrefix(mainstemFile, remotePrefix) {
			return nil
		}
	}
	if _, err := os.Stat(mainstemFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("mainstem file was specified to be at %s but does not exist locally", mainstemFile)
		}
		return fmt.Errorf("failed to stat mainstem file %s: %w", mainstemFile, err)
	}
	return nil
}

func Harvest(ctx context.Context, client *http.Client, minioConfig config.MinioConfig, args HarvestCmd, sitemapIndex string) ([]pkg.SitemapCrawlStats, error) {
	if sitemapIndex == "" {
		return nil, fmt.Errorf("sitemap index must be provided")
	}
	if err := validateMainstemFile(args.MainstemFile); err != nil {
		return nil, err
	}
	index, err := crawl.NewSitemapIndex(sitemapIndex, client)
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
		if err := minioS3.SetupBuckets(); err != nil {
			return nil, err
		}
		storageDestination = minioS3
	}

	return index.
		WithStorageDestination(storageDestination).
		WithConcurrencyConfig(args.ConcurrentSitemaps, args.SitemapWorkers).
		WithSpecifiedSourceFilter(args.Source).
		WithHeadlessChromeUrl(args.HeadlessChromeUrl).
		WithShaclValidationConfig(args.ShaclEndpoint, args.ShaclLocal, args.ExitOnShaclFailure).
		WithMainstemFile(args.MainstemFile).
		HarvestSitemaps(ctx, client)
}
