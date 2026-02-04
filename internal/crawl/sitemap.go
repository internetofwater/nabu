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

	"github.com/internetofwater/nabu/internal/crawl/url_info"
	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"github.com/piprate/json-gold/ld"
	"golang.org/x/sync/errgroup"
)

// Represents an XML sitemap
// https://geoconnex.us/sitemap/usgs/hydrologic-unit__0.xml is an example of a sitemap
type Sitemap struct {
	XMLName xml.Name       `xml:":urlset"`
	URL     []url_info.URL `xml:":url"`

	// The url to the sitemap itself
	sitemapUrl string `xml:"-"`
	// The unique identifier for the sitemap
	// essentially just a serialized version of the path
	// in the URL without the hostname, special characters,
	// / or the final .xml
	sitemapId string `xml:"-"`

	// Whether or not this sitemap is a bulk sitemap
	// and contains links to docker for running container operations instead of individual pids
	isBulkSitemap bool `xml:"-"`

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
	grpcConn   *grpc.ClientConn
	jsonLdProc *ld.JsonLdProcessor
	jsonLdOpt  *ld.JsonLdOptions
	// before downloading a site, send a head request to the server
	// to get its hash and if it already exists in storage, skip it
	checkExistenceBeforeCrawl *atomic.Bool
	storageDestination        storage.CrawlStorage
	exitOnShaclFailure        bool
	// shacl errors can be quite verbose and often very duplicative;
	// this is the maximum of them to store in the crawl report
	maxShaclErrorsToStore int
	// cleanup any jsonld in the last dir in the path
	// that wasn't found during the sitemap crawl
	cleanupOutdatedJsonld bool
	// the number of failed sites in a row before we exit
	// and assume the sitemap is down
	failedSitesToAssumeSitemapDown int
}

// Make a new SiteHarvestConfig with all the clients and config
// initialized and ready to crawl a sitemap
// this config is shared across all goroutines and thus must be thread safe
func NewSitemapHarvestConfig(httpClient *http.Client, sitemap *Sitemap, shaclAddress string, exitOnShaclFailure bool, cleanupOutdatedJsonld bool) (SitemapHarvestConfig, error) {

	if sitemap.workers < 1 {
		return SitemapHarvestConfig{}, fmt.Errorf("no workers set for sitemap %s", sitemap.sitemapId)
	}

	var robotsTxt *robotstxt.Group
	// don't check robots.txt for bulk sitemaps
	// since they point to docker images and not individual web pages to crawl
	if !sitemap.isBulkSitemap {
		firstUrl := sitemap.URL[0]
		robotsTxt, err := newRobots(httpClient, firstUrl.Loc)
		if err != nil {
			return SitemapHarvestConfig{}, err
		}
		if !robotsTxt.Test(common.HarvestAgent) {
			return SitemapHarvestConfig{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
		}
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
		robots:                    robotsTxt,
		httpClient:                httpClient,
		grpcClient:                &grpcClient,
		grpcConn:                  conn,
		jsonLdProc:                JsonLdProc,
		jsonLdOpt:                 JsonLdOpts,
		storageDestination:        sitemap.storageDestination,
		checkExistenceBeforeCrawl: &checkJsonldExistsBeforeDownloading,
		exitOnShaclFailure:        exitOnShaclFailure,
		cleanupOutdatedJsonld:     cleanupOutdatedJsonld,
		workers:                   sitemap.workers,
		// currently hard coded. could be configurable in the future
		maxShaclErrorsToStore: 20,
		// currently hard coded. could be configurable in the future
		failedSitesToAssumeSitemapDown: 20,
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

// given the sitemap identifier and the url return the path to store it
func urlToStoragePath(sitemapId string, url url_info.URL) (string, error) {
	if url.Base64Loc == "" {
		return "", fmt.Errorf("no base64 loc for url %s", url.Loc)
	}
	return fmt.Sprintf("summoned/%s/%s.jsonld", sitemapId, url.Base64Loc), nil
}

func (s *Sitemap) Harvest(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, []string, error) {
	if err := s.ensureValid(config.workers); err != nil {
		return pkg.SitemapCrawlStats{}, nil, err
	}

	if s.isBulkSitemap {
		return s.HarvestBulkSitemap(ctx, config)
	} else {
		return s.HarvestPIDsSitemap(ctx, config)
	}
}

// Harvest all the URLs in the given sitemap and return the associated metadata as well as a list
// of sites that were cleaned up after harvesting
func (s *Sitemap) HarvestPIDsSitemap(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, []string, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", s.sitemapId))
	defer span.End()

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	if config.grpcConn != nil {
		defer func() { _ = config.grpcConn.Close() }()
	}

	start := time.Now()
	log.Infof("Harvesting sitemap %s with %d urls", s.sitemapId, len(s.URL))

	sitemapStatusTracker := NewSitemapStatusTracker(config.failedSitesToAssumeSitemapDown)

	successfulSitesMu := sync.Mutex{}
	// includes both sites that were download
	// and sites that were skipped due to having a matching hash
	successfulSites := make(storage.Set)

	// the number of sites that were hit with a fetch request
	// regardless of whether or not they returned an error
	totalSitesContacted := atomic.Int64{}

	sitesInSitemap := make(storage.Set)

	sitesWithShaclFailures := atomic.Int32{}

	noPreviousData, err := s.storageDestination.IsEmptyDir("summoned/" + s.sitemapId)
	if err != nil {
		return pkg.SitemapCrawlStats{}, nil, err
	}

	if noPreviousData {
		log.Infof("No pre-existing JSON-LD files found in %s so skipping hash checks for already harvested sites", "summoned/"+s.sitemapId)
		config.checkExistenceBeforeCrawl.Store(false)
	} else {
		config.checkExistenceBeforeCrawl.Store(true)
	}

	for _, url := range s.URL {

		if path, err := urlToStoragePath(s.sitemapId, url); err != nil {
			return pkg.SitemapCrawlStats{}, nil, err
		} else {
			sitesInSitemap.Add(path)
		}
		group.Go(func() error {
			if sitemapStatusTracker.AppearsDown() {
				return &SitemapAppearsDownError{
					message: fmt.Sprintf("Returning early since %d failures were detected without a single successful harvest; the sitemap is assumed to be down or had a change in the underlying API", config.failedSitesToAssumeSitemapDown),
				}
			}

			result_metadata, err := harvestOnePID(ctx, s.sitemapId, url, config)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Error(err)
				}
				return err
			}

			totalSitesContacted.Store((totalSitesContacted.Add(1)))

			if !result_metadata.nonFatalError.IsNil() {
				s.errorMu.Lock()
				s.nonFatalErrors = append(s.nonFatalErrors, result_metadata.nonFatalError)
				s.errorMu.Unlock()
				sitemapStatusTracker.AddSiteFailure()
			} else {
				sitemapStatusTracker.AddSiteSuccess()
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
			if result_metadata.pathInStorage != "" {
				successfulSitesMu.Lock()
				if successfulSites.Contains(result_metadata.pathInStorage) {
					successfulSitesMu.Unlock()
					errMsg := fmt.Sprintf("Got at least two responses in the same sitemap crawl that resolved to the same path in storage: %s. URL %s has potential duplicate data in API", result_metadata.pathInStorage, url.Loc)
					log.Error(errMsg)
					return pkg.UrlCrawlError{Url: url.Loc, Message: errMsg}
				}
				successfulSites.Add(result_metadata.pathInStorage)
				successfulSitesMu.Unlock()
			}
			if !result_metadata.serverHadHash && config.checkExistenceBeforeCrawl.Load() {
				// if the server didn't provide a hash then we can skip the hash check
				// since presumably the server doesn't support this header in the HEAD request
				config.checkExistenceBeforeCrawl.Store(false)
				log.Warnf("Server didn't provide a hash on %s. Skipping hash checks going forward for harvested sites", url.Loc)
			}
			if math.Mod(float64(totalSitesContacted.Load()), 500) == 0 {
				log.Infof("Harvested %d/%d sites for %s", totalSitesContacted.Load(), len(s.URL), s.sitemapId)
			}

			return nil
		})
	}
	err = group.Wait()

	stats := pkg.SitemapCrawlStats{
		SitemapSourceLink: s.sitemapUrl,
		SecondsToComplete: time.Since(start).Seconds(),
		SitemapName:       s.sitemapId,
		SuccessfulSites:   len(successfulSites),
		SitesInSitemap:    len(s.URL),
		WarningStats: pkg.WarningReport{
			TotalShaclFailures: int(sitesWithShaclFailures.Load()),
			ShaclWarnings:      s.warnings,
		},
		CrawlFailures: s.nonFatalErrors,
	}

	if err != nil {
		// we still return the stats if there is a failure
		// so that a caller can decide what to log
		return stats, nil, err
	}

	cleanedUpFiles := []string{}
	if config.cleanupOutdatedJsonld {
		log.Info("Cleaning up outdated JSON-LD files in summoned/" + s.sitemapId)
		cleanedUpFiles, err = storage.CleanupFiles("summoned/"+s.sitemapId, sitesInSitemap, s.storageDestination)
		if err != nil {
			log.Error(err)
		} else {
			log.Infof("Cleaned up %d outdated JSON-LD files in summoned/%s", len(cleanedUpFiles), s.sitemapId)
		}
	} else {
		log.Warnf("Skipping old JSON-LD cleanups. It is possible %s will contain outdated JSON-LD files", "summoned/"+s.sitemapId)
	}

	asJson, err := stats.ToJsonIoReader()
	if err != nil {
		return pkg.SitemapCrawlStats{}, nil, err
	}
	err = s.storageDestination.StoreMetadata(fmt.Sprintf("metadata/sitemaps/%s.json", s.sitemapId), asJson)
	if err != nil {
		return pkg.SitemapCrawlStats{}, nil, err
	}

	log.Infof("Finished crawling sitemap %s in %f seconds", s.sitemapId, stats.SecondsToComplete)

	log.Infof("Sitemap %s had %d harvested urls, %d non fatal crawl errors, and %d shacl issues", s.sitemapId, stats.SuccessfulSites, len(stats.CrawlFailures), stats.WarningStats.TotalShaclFailures)

	return stats, cleanedUpFiles, err
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

	urls := make([]url_info.URL, 0)

	resp, err := client.Get(sitemapURL)
	if err != nil {
		return &serializedSitemap, err
	}
	defer func() { _ = resp.Body.Close() }()

	if err = sitemap.Parse(resp.Body, func(entry sitemap.Entry) error {
		urls = append(urls, *url_info.NewUrlFromSitemapEntry(entry))
		return nil
	}); err != nil {
		return &serializedSitemap, err
	}

	serializedSitemap.URL = urls

	return &serializedSitemap, nil
}
