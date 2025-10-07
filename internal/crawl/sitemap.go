// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/temoto/robotstxt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"github.com/piprate/json-gold/ld"
	"golang.org/x/sync/errgroup"
)

// Represents an XML sitemap
// https://geoconnex.us/sitemap/usgs/hydrologic-unit__0.xml is an example of a sitemap
type Sitemap struct {
	XMLName xml.Name `xml:":urlset"`
	URL     []URL    `xml:":url"`

	sitemapUrl string `xml:"-"`
	sitemapId  string `xml:"-"`

	// Strategy used for storing crawled data
	// - explicitly ignores xml marshaling
	// since this is not an xml field but rather
	// associated data with the sitemap struct
	storageDestination storage.CrawlStorage `xml:"-"`

	// channels for passing messages from goroutines
	// that inform on the status of the sitemap crawl
	nonFatalErrors []pkg.UrlCrawlError `xml:"-"`
	errorMu        sync.Mutex

	warnings  []pkg.ShaclInfo `xml:"-"`
	warningMu sync.Mutex

	// the number of parallel workers to use when harvesting the sitemap
	// i.e. 1 worker = 1 goroutine = 1 URL
	workers int `xml:"-"`
}

// all the of the clients and config needed to harvest a particular site
// in a sitemap; these are reused across every site in a sitemap
type SitemapHarvestConfig struct {
	// the number of parallel workers to use when harvesting the sitemap
	workers int
	// the config for the robotstxt behavior
	robots *robotstxt.Group
	// the config for http requests
	httpClient *http.Client
	// the config for grpc requests
	grpcClient *protoBuild.ShaclValidatorClient
	// the grpc connection itself for connecting with the shacl validator
	grpcConn                  *grpc.ClientConn
	jsonLdProc                *ld.JsonLdProcessor
	jsonLdOpt                 *ld.JsonLdOptions
	checkExistenceBeforeCrawl *atomic.Bool
	storageDestination        storage.CrawlStorage
	exitOnShaclFailure        bool
	maxShaclErrorsToStore     int
	cleanupOldJsonld          bool
}

// Make a new SiteHarvestConfig with all the clients and config
// initialized and ready to crawl a sitemap
// this config is shared across all goroutines and thus must be thread safe
func NewSitemapHarvestConfig(httpClient *http.Client, sitemap *Sitemap, shaclAddress string, exitOnShaclFailure bool, cleanupOldJsonld bool) (SitemapHarvestConfig, error) {

	if sitemap.workers < 1 {
		return SitemapHarvestConfig{}, fmt.Errorf("no workers set for sitemap %s", sitemap.sitemapId)
	}

	firstUrl := sitemap.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return SitemapHarvestConfig{}, err
	}
	if !robotstxt.Test(gleanerAgent) {
		return SitemapHarvestConfig{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
	}

	var conn *grpc.ClientConn
	var grpcClient protoBuild.ShaclValidatorClient
	// shacl validation is optional
	if shaclAddress != "" {
		// 32 megabytes is the current upperbound of the jsonld documents we will validate
		// beyond that is a sign that the document may be too large or incorrectly formatted
		thirtyTwoMB := 32 * 1024 * 1024
		conn, err := grpc.NewClient(shaclAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithMaxHeaderListSize(uint32(thirtyTwoMB)),
		)
		if err != nil {
			return SitemapHarvestConfig{}, fmt.Errorf("failed to connect to gRPC server: %w", err)
		}
		grpcClient = protoBuild.NewShaclValidatorClient(conn)
	} else {
		conn = nil
		grpcClient = nil
	}

	JsonLdProc, JsonLdOpts, err := common.NewJsonldProcessor(true, make(map[string]string))
	if err != nil {
		return SitemapHarvestConfig{}, fmt.Errorf("failed to create JSON-LD processor: %w", err)
	}

	checkJsonldExistsBeforeDownloading := atomic.Bool{}
	checkJsonldExistsBeforeDownloading.Store(true)

	return SitemapHarvestConfig{
		robots:                    robotstxt,
		httpClient:                httpClient,
		grpcClient:                &grpcClient,
		grpcConn:                  conn,
		jsonLdProc:                JsonLdProc,
		jsonLdOpt:                 JsonLdOpts,
		storageDestination:        sitemap.storageDestination,
		checkExistenceBeforeCrawl: &checkJsonldExistsBeforeDownloading,
		exitOnShaclFailure:        exitOnShaclFailure,
		cleanupOldJsonld:          cleanupOldJsonld,
		workers:                   sitemap.workers,
		// currently hard coded. will be configurable in the future
		maxShaclErrorsToStore: 20,
	}, nil
}

// make sure the config is sane	before we start
func (s *Sitemap) ensureValid(workers int) error {
	// For the time being, we assume that the first URL in the sitemap has the
	// same robots.txt as the rest of the items
	if len(s.URL) == 0 {
		return fmt.Errorf("no URLs found in sitemap")
	} else if s.storageDestination == nil {
		return fmt.Errorf("no storage destination set")
	} else if workers < 1 {
		return fmt.Errorf("no workers set")
	}
	return nil
}

// Harvest all the URLs in the given sitemap
func (s *Sitemap) Harvest(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", s.sitemapId))
	defer span.End()

	if err := s.ensureValid(config.workers); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	if config.grpcConn != nil {
		defer func() { _ = config.grpcConn.Close() }()
	}

	start := time.Now()
	log.Infof("Harvesting sitemap %s with %d urls", s.sitemapId, len(s.URL))

	sitesHarvested := atomic.Int32{}
	sitesWithShaclFailures := atomic.Int32{}

	noPreviousData, err := s.storageDestination.IsEmptyDir("summoned/" + s.sitemapId)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if noPreviousData {
		log.Infof("No pre-existing JSON-LD files found in %s so skipping hash checks for already harvested sites", "summoned/"+s.sitemapId)
		config.checkExistenceBeforeCrawl.Store(false)
	} else {
		config.checkExistenceBeforeCrawl.Store(true)
	}

	for _, url := range s.URL {
		group.Go(func() error {
			result_metadata, err := harvestOneSite(ctx, s.sitemapId, url, config)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Error(err)
				}
				return err
			}
			if !result_metadata.nonFatalError.IsNil() {
				s.errorMu.Lock()
				s.nonFatalErrors = append(s.nonFatalErrors, result_metadata.nonFatalError)
				s.errorMu.Unlock()
			}
			if !result_metadata.warning.IsNil() {
				shaclFailuresSoFar := sitesWithShaclFailures.Load()
				if shaclFailuresSoFar < int32(config.maxShaclErrorsToStore) {
					log.Errorf("Shacl validation failed for %s: %s", url.Loc, result_metadata.warning)
					s.warningMu.Lock()
					s.warnings = append(s.warnings, result_metadata.warning)
					s.warningMu.Unlock()
				} else if shaclFailuresSoFar == int32(config.maxShaclErrorsToStore) {
					log.Warnf("Too many shacl errors for %s. Skipping further errors to prevent log spam", s.sitemapId)
				}
				if result_metadata.warning.ShaclStatus == pkg.ShaclInvalid {
					sitesWithShaclFailures.Store(
						shaclFailuresSoFar + 1,
					)
				}
			}
			sitesHarvested.Add(1)
			if !result_metadata.serverHadHash && config.checkExistenceBeforeCrawl.Load() {
				// if the server didn't provide a hash then we can skip the hash check
				// since presumably the server doesn't support this header in the HEAD request
				config.checkExistenceBeforeCrawl.Store(false)
				log.Warn("Server didn't provide a hash for checking so skipping hash checks for harvested sites")
			}
			if math.Mod(float64(sitesHarvested.Load()), 500) == 0 {
				log.Infof("Harvested %d/%d sites for %s", sitesHarvested.Load(), len(s.URL), s.sitemapId)
			}

			return nil
		})
	}
	if err = group.Wait(); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if config.cleanupOldJsonld {
		go func() {
			dir := "summoned/" + s.sitemapId
			log.Infof("Cleaning up old JSON-LD files in %s", dir)
			files, err := s.storageDestination.ListDir(dir)
			if err != nil {
				log.Error(err)
				return
			}
			var deleteTotal int
			for k, v := range files {
				if !files.Contains(k) {
					if err := s.storageDestination.Remove(fmt.Sprintf("summoned/%s/%s", s.sitemapId, v)); err != nil {
						log.Error(err)
					}
					deleteTotal++
				}
			}
			log.Infof("Json-LD cleanup complete, deleted %d files", deleteTotal)
		}()
	}

	stats := pkg.SitemapCrawlStats{
		SitemapSourceLink: s.sitemapUrl,
		SecondsToComplete: time.Since(start).Seconds(),
		SitemapName:       s.sitemapId,
		SitesHarvested:    int(sitesHarvested.Load()),
		SitesInSitemap:    len(s.URL),
		WarningStats: pkg.WarningReport{
			TotalShaclFailures: int(sitesWithShaclFailures.Load()),
			ShaclWarnings:      s.warnings,
		},
		CrawlFailures: s.nonFatalErrors,
	}
	asJson, err := stats.ToJsonIoReader()
	if err != nil {
		log.Fatal(err)
	}
	err = s.storageDestination.StoreMetadata(fmt.Sprintf("metadata/sitemaps/%s.json", s.sitemapId), asJson)
	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("Finished crawling sitemap %s in %f seconds", s.sitemapId, stats.SecondsToComplete)

	log.Infof("Sitemap %s had %d harvested urls, %d non fatal crawl errors, and %d shacl issues", s.sitemapId, stats.SitesHarvested, len(stats.CrawlFailures), stats.WarningStats.TotalShaclFailures)

	return stats, err
}

// Represents a URL tag and its attributes within a sitemap
type URL struct {
	Loc        string  `xml:"loc"`
	LastMod    string  `xml:"lastmod"`
	ChangeFreq string  `xml:"changefreq"`
	Priority   float32 `xml:"priority"`
}

// Given a sitemap url, return a Sitemap object
func NewSitemap(ctx context.Context, client *http.Client, sitemapURL string, workers int, storageDestination storage.CrawlStorage, sitemapId string) (*Sitemap, error) {
	if workers == 0 {
		return &Sitemap{}, fmt.Errorf("no workers set in sitemap for %s", sitemapURL)
	}

	serializedSitemap := Sitemap{workers: workers,
		storageDestination: storageDestination,
		sitemapId:          sitemapId,
		sitemapUrl:         sitemapURL,
		nonFatalErrors:     []pkg.UrlCrawlError{},
		warnings:           []pkg.ShaclInfo{},
	}

	urls := make([]URL, 0)

	resp, err := client.Get(sitemapURL)
	if err != nil {
		return &serializedSitemap, err
	}
	defer func() { _ = resp.Body.Close() }()

	err = sitemap.Parse(resp.Body, func(entry sitemap.Entry) error {
		url := URL{}
		url.Loc = strings.TrimSpace(entry.GetLocation())
		urls = append(urls, url)
		return nil
	})

	if err != nil {
		return &serializedSitemap, err
	}

	serializedSitemap.URL = urls

	return &serializedSitemap, nil
}
