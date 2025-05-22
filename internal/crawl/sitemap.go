// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/config"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
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

// Given a response, get the jsonld within the response
// it will first try to get the jsonld directly if the content
// type is application/ld+json otherwise it tries to find it
// inside the html
func getJSONLD(resp *http.Response, url URL, body []byte) ([]byte, error) {
	mime := resp.Header.Get("Content-Type")
	if !strings.Contains(mime, "application/ld+json") {
		if strings.Contains(mime, "text/html") {
			jsonldString, err := GetJsonLDFromHTML(body)
			if err != nil {
				log.Errorf("failed to parse jsonld within the html for %s", url.Loc)
				return nil, err
			}
			return []byte(jsonldString), nil
		} else {
			errormsg := fmt.Sprintf("got wrong file type %s for %s", mime, url.Loc)
			log.Error(errormsg)
			return nil, UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
		}
	}
	return body, nil
}

// Harvest all the URLs in the sitemap
func (s Sitemap) Harvest(ctx context.Context, workers int, sitemapID string, validateShacl bool) (SitemapCrawlStats, error) {
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("sitemap_harvest_%s", sitemapID))
	defer span.End()

	var group errgroup.Group
	group.SetLimit(workers)

	// For the time being, we assume that the first URL in the sitemap has the
	// same robots.txt as the rest of the items
	if len(s.URL) == 0 {
		return SitemapCrawlStats{}, fmt.Errorf("no URLs found in sitemap")
	} else if s.storageDestination == nil {
		return SitemapCrawlStats{}, fmt.Errorf("no storage destination set")
	} else if workers < 1 {
		return SitemapCrawlStats{}, fmt.Errorf("no workers set")
	}

	firstUrl := s.URL[0]
	robotstxt, err := newRobots(firstUrl.Loc)
	if err != nil {
		return SitemapCrawlStats{}, err
	}
	if !robotstxt.Test(gleanerAgent) {
		return SitemapCrawlStats{}, fmt.Errorf("robots.txt does not allow us to crawl %s", firstUrl.Loc)
	}

	crawlErrorChan := make(chan UrlCrawlError, len(s.URL))
	client := common.NewRetryableHTTPClient()

	conn, err := grpc.NewClient("unix:///tmp/shacl_validator.sock",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return SitemapCrawlStats{}, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}
	defer conn.Close()
	grpcClient := protoBuild.NewShaclValidatorClient(conn)

	JsonLdProc, JsonLdOpts, err := common.NewJsonldProcessor(true, []config.ContextMap{})
	if err != nil {
		return SitemapCrawlStats{}, fmt.Errorf("failed to create JSON-LD processor: %w", err)
	}

	start := time.Now()
	for _, url := range s.URL {
		// Capture the URL for use in the goroutine.
		url := url
		group.Go(func() error {
			// Create a new span for each URL and propagate the updated context
			ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("fetch_%s", url.Loc))
			defer span.End()

			req, err := http.NewRequest("GET", url.Loc, nil)
			if err != nil {
				return err
			}
			req.Header.Set("User-Agent", gleanerAgent)
			req.Header.Set("Accept", "application/ld+json")

			resp, err := client.Do(req)
			if err != nil {
				return err
			}

			defer resp.Body.Close()

			if resp.StatusCode >= 400 {
				errormsg := fmt.Sprintf("failed to fetch %s, got status %s", url.Loc, resp.Status)
				log.Error(errormsg)
				// status makes jaeger mark as failed with red, whereas SetEvent just marks it with a message
				span.SetStatus(codes.Error, errormsg)
				crawlErrorChan <- UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
				return nil
			}

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read response body: %w", err)
			}

			jsonld, err := getJSONLD(resp, url, bodyBytes)
			if err != nil {
				// If it's a UrlCrawlError, store it for stats
				// put don't return it, since it is non fatal
				if urlErr, ok := err.(UrlCrawlError); ok {
					span.SetStatus(codes.Error, urlErr.Message)
					crawlErrorChan <- urlErr
					return nil
				}
				return fmt.Errorf("failed to get JSON-LD from response: %w", err)
			}

			triples, err := common.JsonldToNQ(string(jsonld), JsonLdProc, JsonLdOpts)
			if err != nil {
				return fmt.Errorf("failed to convert JSON-LD to N-Quads: %w", err)
			}

			// To generate a hash we need to copy the response body
			itemHash, err := generateHashFilename(jsonld)
			if err != nil {
				return err
			}

			summonedPath := fmt.Sprintf("summoned/%s/%s", sitemapID, itemHash)

			exists, err := s.storageDestination.Exists(summonedPath)
			if err != nil {
				return err
			}
			if exists {
				return nil
			}

			if validateShacl {
				ctx, grpcSubspan := opentelemetry.SubSpanFromCtxWithName(ctx, "grpc_shacl_validation")
				log.Debugf("validating triples of byte size %d", len(triples))
				if reply, err := grpcClient.Validate(ctx, &protoBuild.TurtleValidationRequest{Triples: triples}); err != nil {
					grpcSubspan.End()
					return fmt.Errorf("failed sending validation request to gRPC server: %w", err)
				} else if !reply.Valid {
					grpcSubspan.SetStatus(codes.Error, reply.Message)
					log.Errorf("SHACL validation failed for %s: %s", url.Loc, reply.Message)
					crawlErrorChan <- UrlCrawlError{Url: url.Loc, ShaclValid: reply.Valid, ShaclErrorMessage: reply.Message}
					grpcSubspan.End()
					return nil
				}
				grpcSubspan.End()
			}

			// Store from the buffered copy
			if err = s.storageDestination.Store(summonedPath, bytes.NewReader(jsonld)); err != nil {
				return err
			}

			if robotstxt.CrawlDelay > 0 {
				log.Debug("sleeping for", robotstxt.CrawlDelay)
				time.Sleep(robotstxt.CrawlDelay)
			}

			return nil
		})
	}
	err = group.Wait()

	stats := SitemapCrawlStats{SecondsToComplete: time.Since(start).Seconds(), SitemapName: sitemapID}
	// we close this here to make sure we can range without blocking
	// We know we can close this since we have already waited on all go routines
	close(crawlErrorChan)
	for err := range crawlErrorChan {
		stats.CrawlFailures = append(stats.CrawlFailures, err)
	}

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
