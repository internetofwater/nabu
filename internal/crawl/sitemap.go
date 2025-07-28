// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"fmt"
	"maps"
	"math"
	"net/http"
	"slices"
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
type Sitemap struct {
	XMLName xml.Name `xml:":urlset"`
	URL     []URL    `xml:":url"`

	// Strategy used for storing crawled data
	// - explicitly ignores xml marshaling
	// since this is not an xml field but rather
	// associated data with the sitemap struct
	storageDestination storage.CrawlStorage `xml:"-"`
}

// all the of the clients and config needed to harvest a particular site
// in a sitemap; these are reused across every site in a sitemap
type SitemapHarvestConfig struct {
	robots                    *robotstxt.Group
	httpClient                *http.Client
	grpcClient                *protoBuild.ShaclValidatorClient
	grpcConn                  *grpc.ClientConn
	jsonLdProc                *ld.JsonLdProcessor
	jsonLdOpt                 *ld.JsonLdOptions
	nonFatalErrorChan         chan pkg.UrlCrawlError
	storageDestination        storage.CrawlStorage
	checkExistenceBeforeCrawl atomic.Bool
}

// Make a new SiteHarvestConfig with all the clients and config
// initialized and ready to crawl a sitemap
// this config is shared across all goroutines and thus must be thread safe
func NewSitemapHarvestConfig(httpClient *http.Client, sitemap Sitemap, validateShacl bool) (SitemapHarvestConfig, error) {

	firstUrl := sitemap.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return SitemapHarvestConfig{}, err
	}
	if !robotstxt.Test(gleanerAgent) {
		return SitemapHarvestConfig{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
	}

	crawlErrorChan := make(chan pkg.UrlCrawlError, len(sitemap.URL))

	var conn *grpc.ClientConn
	var grpcClient protoBuild.ShaclValidatorClient
	// shacl validation is optional
	if validateShacl {
		conn, err := grpc.NewClient("unix:///tmp/shacl_validator.sock",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	return SitemapHarvestConfig{
		robots:             robotstxt,
		httpClient:         httpClient,
		grpcClient:         &grpcClient,
		grpcConn:           conn,
		jsonLdProc:         JsonLdProc,
		jsonLdOpt:          JsonLdOpts,
		nonFatalErrorChan:  crawlErrorChan,
		storageDestination: sitemap.storageDestination,
	}, nil
}

// make sure the config is sane	before we start
func (s Sitemap) ensureValid(workers int) error {
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
func (s Sitemap) Harvest(ctx context.Context, client *http.Client, workers int, sitemapID string, validateShacl bool, cleanupOldJsonld bool) (pkg.SitemapCrawlStats, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapID))
	defer span.End()

	var group errgroup.Group
	group.SetLimit(workers)

	if err := s.ensureValid(workers); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	sitemapHarvestConf, err := NewSitemapHarvestConfig(client, s, validateShacl)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if sitemapHarvestConf.grpcConn != nil {
		defer func() { _ = sitemapHarvestConf.grpcConn.Close() }()
	}

	start := time.Now()
	log.Infof("Harvesting sitemap %s with %d urls", sitemapID, len(s.URL))

	sitesHarvested := atomic.Int32{}

	successfulUrls := make(map[string]string) // preallocate for performance
	var urlMutex sync.Mutex

	noPreviousData, err := s.storageDestination.IsEmptyDir("summoned/" + sitemapID)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if noPreviousData {
		log.Infof("No pre-existing JSON-LD files found in %s so skipping hash checks for already harvested sites", "summoned/"+sitemapID)
		sitemapHarvestConf.checkExistenceBeforeCrawl.Store(false)
	} else {
		sitemapHarvestConf.checkExistenceBeforeCrawl.Store(true)
	}

	for _, url := range s.URL {
		group.Go(func() error {
			locationInStorage, serverProvidedHashForChecking, err := harvestOneSite(ctx, sitemapID, url, &sitemapHarvestConf)
			sitesHarvested.Add(1)
			if !serverProvidedHashForChecking && sitemapHarvestConf.checkExistenceBeforeCrawl.Load() {
				// if the server didn't provide a hash then we can skip the hash check
				// since presumably the server doesn't support this header in the HEAD request
				sitemapHarvestConf.checkExistenceBeforeCrawl.Store(false)
				log.Warn("Server didn't provide a hash for checking so skipping hash checks for harvested sites")
			}

			if math.Mod(float64(sitesHarvested.Load()), 1000) == 0 {
				log.Debugf("Harvested %d/%d sites for %s", sitesHarvested.Load(), len(s.URL), sitemapID)
			}
			if err == nil {
				urlMutex.Lock()
				successfulUrls[url.Loc] = locationInStorage
				urlMutex.Unlock()
			}
			return err
		})
	}
	if err = group.Wait(); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if cleanupOldJsonld {
		go func() {
			dir := "summoned/" + sitemapID
			log.Infof("Cleaning up old JSON-LD files in %s", dir)
			files, err := s.storageDestination.ListDir(dir)
			if err != nil {
				log.Error(err)
				return
			}
			var deleteTotal int
			for k, v := range files {
				if !files.Contains(k) {
					if err := s.storageDestination.Remove(fmt.Sprintf("summoned/%s/%s", sitemapID, v)); err != nil {
						log.Error(err)
					}
					deleteTotal++
				}
			}
			log.Infof("Json-LD cleanup complete, deleted %d files", deleteTotal)
		}()
	}

	stats := pkg.SitemapCrawlStats{
		SuccessfulUrls:    slices.Collect(maps.Keys(successfulUrls)),
		SecondsToComplete: time.Since(start).Seconds(),
		SitemapName:       sitemapID,
		SitesHarvested:    int(sitesHarvested.Load()),
		SitesInSitemap:    len(s.URL),
	}
	// we close this here to make sure we can range without blocking
	// We know we can close this since we have already waited on all go routines
	close(sitemapHarvestConf.nonFatalErrorChan)
	for nonFatalErr := range sitemapHarvestConf.nonFatalErrorChan {
		stats.CrawlFailures = append(stats.CrawlFailures, nonFatalErr)
	}

	go func() {
		asJson, err := stats.ToJsonIoReader()
		if err != nil {
			log.Fatal(err)
		}
		err = s.storageDestination.Store(fmt.Sprintf("metadata/sitemaps/%s.json", sitemapID), asJson)
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Debugf("Finished crawling sitemap %s in %f seconds", sitemapID, stats.SecondsToComplete)

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
func NewSitemap(ctx context.Context, client *http.Client, sitemapURL string) (Sitemap, error) {
	serializedSitemap := Sitemap{}

	urls := make([]URL, 0)

	resp, err := client.Get(sitemapURL)
	if err != nil {
		return serializedSitemap, err
	}
	defer func() { _ = resp.Body.Close() }()

	err = sitemap.Parse(resp.Body, func(entry sitemap.Entry) error {
		url := URL{}
		url.Loc = strings.TrimSpace(entry.GetLocation())
		urls = append(urls, url)
		return nil
	})

	if err != nil {
		return serializedSitemap, err
	}

	serializedSitemap.URL = urls
	return serializedSitemap, nil
}
