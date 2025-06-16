// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"encoding/xml"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/temoto/robotstxt"
	"go.opentelemetry.io/otel/trace"
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
	robots             *robotstxt.Group
	httpClient         *http.Client
	grpcClient         *protoBuild.ShaclValidatorClient
	grpcConn           *grpc.ClientConn
	jsonLdProc         *ld.JsonLdProcessor
	jsonLdOpt          *ld.JsonLdOptions
	nonFatalErrorChan  chan pkg.UrlCrawlError
	storageDestination storage.CrawlStorage
}

// Make a new SiteHarvestConfig with all the clients and config
// initialized and ready to crawl a sitemap
// this config is shared across all goroutines and thus must be thread safe
func NewSitemapHarvestConfig(sitemap Sitemap, validateShacl bool) (SitemapHarvestConfig, error) {

	firstUrl := sitemap.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return SitemapHarvestConfig{}, err
	}
	if !robotstxt.Test(gleanerAgent) {
		return SitemapHarvestConfig{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
	}

	crawlErrorChan := make(chan pkg.UrlCrawlError, len(sitemap.URL))

	// create a client that is custom tuned for high throughput
	// crawling; for some reason yourls doesn't respond well to the
	// opentelemetry headers; so we do any otel events manually via
	// transport hooks
	client := &http.Client{
		// a feature should not take more than 30 seconds to resolve
		// otherwise it will be skipped
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			// allow for up to 5000 idle connections
			// to the same host so that we can hit yourls
			// by default the go http client limits these to 100
			MaxIdleConns:          0,
			MaxIdleConnsPerHost:   0,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     false, // keep-alives are good for performance
			ForceAttemptHTTP2:     true,
			// set event when connection is established
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				// You can implement custom logic here or use the default dialer
				span := trace.SpanFromContext(ctx)
				if span != nil {
					span.AddEvent("HTTP connection")
				}
				return net.DialTimeout(network, addr, 30*time.Second)
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Add an OpenTelemetry event when a redirect occurs
			span := trace.SpanFromContext(req.Context())
			if span != nil {
				span.AddEvent("HTTP redirect")
			}
			return nil
		},
	}

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

	JsonLdProc, JsonLdOpts, err := common.NewJsonldProcessor(true, []config.ContextMap{})
	if err != nil {
		return SitemapHarvestConfig{}, fmt.Errorf("failed to create JSON-LD processor: %w", err)
	}

	return SitemapHarvestConfig{
		robots:             robotstxt,
		httpClient:         client,
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
func (s Sitemap) Harvest(ctx context.Context, workers int, sitemapID string, validateShacl bool) (pkg.SitemapCrawlStats, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapID))
	defer span.End()

	var group errgroup.Group
	group.SetLimit(workers)

	if err := s.ensureValid(workers); err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	sitemapHarvestConf, err := NewSitemapHarvestConfig(s, validateShacl)
	if err != nil {
		return pkg.SitemapCrawlStats{}, err
	}

	if sitemapHarvestConf.grpcConn != nil {
		defer func() { _ = sitemapHarvestConf.grpcConn.Close() }()
	}

	start := time.Now()
	log.Infof("Harvesting sitemap %s with %d urls", sitemapID, len(s.URL))

	sitesHarvested := atomic.Int64{}

	for _, url := range s.URL {
		// Capture the URL for use in the goroutine.
		url := url
		group.Go(func() error {
			err := harvestOneSite(ctx, sitemapID, url, &sitemapHarvestConf)
			sitesHarvested.Add(1)
			if math.Mod(float64(sitesHarvested.Load()), 1000) == 0 {
				log.Debugf("Harvested %d/%d sites for %s", sitesHarvested.Load(), len(s.URL), sitemapID)
			}
			return err
		})
	}
	err = group.Wait()

	stats := pkg.SitemapCrawlStats{SecondsToComplete: time.Since(start).Seconds(), SitemapName: sitemapID}
	// we close this here to make sure we can range without blocking
	// We know we can close this since we have already waited on all go routines
	close(sitemapHarvestConf.nonFatalErrorChan)
	for err := range sitemapHarvestConf.nonFatalErrorChan {
		stats.CrawlFailures = append(stats.CrawlFailures, err)
	}

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
func NewSitemap(ctx context.Context, sitemapURL string) (Sitemap, error) {
	serializedSitemap := Sitemap{}

	urls := make([]URL, 0)
	err := sitemap.ParseFromSite(sitemapURL, func(entry sitemap.Entry) error {
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
