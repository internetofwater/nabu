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
	"github.com/internetofwater/nabu/internal/mainstems"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/parquettable"
	"github.com/internetofwater/nabu/pkg"
	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	log "github.com/sirupsen/logrus"
	"github.com/temoto/robotstxt"

	"github.com/internetofwater/nabu/internal/crawl/url_info"
	"golang.org/x/sync/errgroup"
)

// Represents an XML sitemap
// https://geoconnex.us/sitemap/usgs/hydrologic-unit__0.xml is an example of a sitemap
type Sitemap struct {
	XMLName xml.Name       `xml:":urlset"`
	URL     []url_info.URL `xml:":url"`

	// Contains all the metadata from the sitemap index about this sitemap; this is not from the sitemap itself but rather from the sitemap index that references this sitemap
	metadata SitemapMetadata `xml:"-"`

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
	// validates harvested jsonld against the SHACL shape; nil skips validation
	shaclValidator ShaclValidator
	// the destination to store the crawled data
	storageDestination storage.CrawlStorage
	// exit immediately if a shacl validation fails
	exitOnShaclFailure bool
	// shacl errors can be quite verbose and often very duplicative;
	// this is the maximum of them to store in the crawl report
	maxShaclErrorsToStore int
	// the number of failed sites in a row before we exit
	// and assume the sitemap is down
	failedSitesToAssumeDatasetDown int
	// associates each feature with a mainstem; nil if the
	// sitemap did not request mainstem associations
	mainstemService mainstems.MainstemService
}

// Add extra information to a harvested feature. Enrichments are written into the JSON-LD
// so that they are kept when it is converted to RDF, and may also be put in their own
// column so they can be used without parsing the JSON-LD
func enrichFeature(ctx context.Context, config *SitemapHarvestConfig, feature *parquettable.Feature) error {
	return addMainstemToFeature(ctx, config, feature)
}

// Add the mainstem associated with the geometry of a feature if the sitemap requested mainstem associations
func addMainstemToFeature(ctx context.Context, config *SitemapHarvestConfig, feature *parquettable.Feature) error {
	if config.mainstemService == nil {
		return nil
	}
	mainstemURI, err := mainstems.GetMainstemURIForWkb(ctx, config.mainstemService, feature.Geometry)
	if err != nil {
		return fmt.Errorf("failed to get the mainstem for the document harvested from %s: %w", feature.URL, err)
	}
	if mainstemURI == "" {
		return nil
	}
	feature.MainstemURI = mainstemURI
	enriched, err := mainstems.AddMainstemToJsonld(feature.JSONLD, mainstemURI)
	if errors.Is(err, mainstems.ErrCannotAddMainstem) {
		log.Warnf("Only adding the mainstem for %s to the %s column: %v", feature.URL, parquettable.ColumnMainstemURI, err)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to add the mainstem to the JSON-LD harvested from %s: %w", feature.URL, err)
	}
	feature.JSONLD = enriched
	return nil
}

// Make a new SiteHarvestConfig with all the clients and config
// initialized and ready to crawl a sitemap
// this config is shared across all goroutines and thus must be thread safe
func NewSitemapHarvestConfig(httpClient *http.Client, sitemap *Sitemap, shaclValidator ShaclValidator, exitOnShaclFailure bool) (SitemapHarvestConfig, error) {

	if sitemap.workers < 1 {
		return SitemapHarvestConfig{}, fmt.Errorf("no workers set for sitemap %s", sitemap.metadata.SitemapID)
	}

	var robotsTxt *robotstxt.Group
	// don't check robots.txt for bulk sitemaps
	// since they point to docker images and not individual web pages to crawl
	if !sitemap.metadata.IsBulkSitemap() {
		firstUrl := sitemap.URL[0]
		robotsTxt, err := newRobots(httpClient, firstUrl.Loc)
		if err != nil {
			return SitemapHarvestConfig{}, err
		}
		if !robotsTxt.Test(common.HarvestAgent) {
			return SitemapHarvestConfig{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
		}
	}

	return SitemapHarvestConfig{
		robots:             robotsTxt,
		httpClient:         httpClient,
		shaclValidator:     shaclValidator,
		storageDestination: sitemap.storageDestination,
		exitOnShaclFailure: exitOnShaclFailure,
		workers:            sitemap.workers,
		// currently hard coded. could be configurable in the future
		maxShaclErrorsToStore: 20,
		// currently hard coded. could be configurable in the future
		failedSitesToAssumeDatasetDown: 20,
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

func (s *Sitemap) Harvest(ctx context.Context, config *SitemapHarvestConfig) (pkg.SitemapCrawlStats, error) {
	if err := s.ensureValid(config.workers); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	var stats pkg.SitemapCrawlStats
	var err error
	if s.metadata.IsBulkSitemap() {
		stats, err = s.HarvestBulkSitemap(ctx, config)
	} else {
		stats, err = s.HarvestPIDsSitemap(ctx, config)
	}
	if err != nil {
		log.Errorf("Error harvesting sitemap %s: %s", s.metadata.SitemapID, err)
		return stats, err
	}

	asJson, err := stats.ToJsonIoReader()
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}
	err = s.storageDestination.StoreMetadata(fmt.Sprintf("metadata/sitemaps/%s.json", s.metadata.SitemapID), asJson)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}
	return stats, err
}

// Harvest all the URLs in the given sitemap into a single parquet file and return the associated metadata.
// The file only contains the documents fetched in this harvest; urls that fail with a non fatal error are
// reported in the crawl stats and left out. If the harvest fails, the previous file is kept as is
func (s *Sitemap) HarvestPIDsSitemap(ctx context.Context, config *SitemapHarvestConfig) (crawlStats pkg.SitemapCrawlStats, err error) {
	if s.metadata.SitemapID == "" {
		return pkg.SitemapCrawlStats{}, fmt.Errorf("sitemap id is required for harvesting")
	}

	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", s.metadata.SitemapID))
	defer span.End()

	start := time.Now()
	log.Infof("Harvesting sitemap %s with %d urls", s.metadata.SitemapID, len(s.URL))

	parquetPath := SummonedParquetPath(s.metadata.SitemapID)
	sink, err := newParquetSink(s.storageDestination, parquetPath)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(config.workers)

	sitemapStatusTracker := NewSitemapStatusTracker(config.failedSitesToAssumeDatasetDown)

	sitesMu := sync.Mutex{}
	successfulSites := make(storage.Set)

	// the number of sites that were hit with a fetch request
	// regardless of whether or not they returned an error
	totalSitesContacted := atomic.Int64{}

	sitesWithShaclFailures := atomic.Int32{}

	for _, url := range s.URL {
		group.Go(func() error {
			if sitemapStatusTracker.AppearsDown() {
				return &SitemapAppearsDownError{
					message: fmt.Sprintf("Returning early since %d failures were detected without a single successful harvest; the sitemap is assumed to be down or had a change in the underlying API", config.failedSitesToAssumeDatasetDown),
				}
			}

			result_metadata, err := harvestOnePID(groupCtx, s.metadata.SitemapID, url, config)
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
					log.Warnf("Too many shacl errors for %s. Skipping further errors to prevent log spam", s.metadata.SitemapID)
				}
				if result_metadata.warning.ShaclStatus == pkg.ShaclInvalid {
					sitesWithShaclFailures.Store(
						shaclFailuresSoFar + 1,
					)
				}
			}

			if result_metadata.feature != nil {
				sitesMu.Lock()
				if successfulSites.Contains(url.Loc) {
					sitesMu.Unlock()
					errMsg := fmt.Sprintf("Got at least two responses in the same sitemap crawl for the same url. URL %s has potential duplicate data in API", url.Loc)
					log.Error(errMsg)
					return pkg.UrlCrawlError{Url: url.Loc, Message: errMsg}
				}
				successfulSites.Add(url.Loc)
				sitesMu.Unlock()

				if err := sink.Add(*result_metadata.feature); err != nil {
					return fmt.Errorf("failed to add %s to %s: %w", url.Loc, parquetPath, err)
				}
			}
			if math.Mod(float64(totalSitesContacted.Load()), 500) == 0 {
				log.Infof("Harvested %d/%d sites for %s", totalSitesContacted.Load(), len(s.URL), s.metadata.SitemapID)
			}

			return nil
		})
	}
	err = group.Wait()

	stats := pkg.SitemapCrawlStats{
		SitemapSourceLink:  s.metadata.Loc,
		SecondsToComplete:  time.Since(start).Seconds(),
		SitemapName:        s.metadata.SitemapID,
		SitemapDescription: s.metadata.DatasetDescription,
		SuccessfulSites:    len(successfulSites),
		SitesInSitemap:     len(s.URL),
		WarningStats: pkg.WarningReport{
			TotalShaclFailures: int(sitesWithShaclFailures.Load()),
			ShaclWarnings:      s.warnings,
		},
		CrawlFailures: s.nonFatalErrors,
		DatasetDown:   sitemapStatusTracker.AppearsDown(),
	}

	if err != nil {
		sink.Abort(err)
		// we still return the stats if there is a failure
		// so that a caller can decide what to log
		return stats, err
	}

	rows, err := sink.Commit()
	if errors.Is(err, errNoFeaturesHarvested) {
		log.Warnf("Keeping the previous %s since no documents were harvested from sitemap %s", parquetPath, s.metadata.SitemapID)
		return stats, nil
	} else if err != nil {
		return stats, err
	}

	stats.SecondsToComplete = time.Since(start).Seconds()

	log.Infof("Finished crawling sitemap %s in %f seconds; wrote %d documents to %s", s.metadata.SitemapID, stats.SecondsToComplete, rows, parquetPath)

	log.Infof("Sitemap %s had %d harvested urls, %d non fatal crawl errors, and %d shacl issues", s.metadata.SitemapID, stats.SuccessfulSites, len(stats.CrawlFailures), stats.WarningStats.TotalShaclFailures)

	return stats, nil
}

// Given a sitemap url, return a Sitemap object
func NewSitemap(ctx context.Context, client *http.Client, workers int, storageDestination storage.CrawlStorage, metadata SitemapMetadata) (*Sitemap, error) {
	if workers == 0 {
		return &Sitemap{}, fmt.Errorf("no workers set in sitemap for %s", metadata.Loc)
	}
	if metadata.SitemapID == "" {
		return &Sitemap{}, fmt.Errorf("no sitemap id set in sitemap metadata for sitemap with loc %s", metadata.Loc)
	}
	if metadata.Loc == "" {
		return &Sitemap{}, fmt.Errorf("no location set in sitemap metadata for sitemap with id %s", metadata.SitemapID)
	}

	serializedSitemap := Sitemap{workers: workers,
		storageDestination: storageDestination,
		nonFatalErrors:     []pkg.UrlCrawlError{},
		warnings:           []pkg.ShaclInfo{},
		metadata:           metadata,
	}

	if serializedSitemap.metadata.IsBulkSitemap() {
		log.Infof("Harvesting sitemap %s as bulk sitemap", metadata.SitemapID)
	}

	urls := make([]url_info.URL, 0)

	resp, err := client.Get(metadata.Loc)
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
