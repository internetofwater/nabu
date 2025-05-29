// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

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

// Crawl and download a single URL
func harvestOneSite(ctx context.Context, sitemapId string, url URL, config *SitemapHarvestConfig) error {
	// Create a new span for each URL and propagate the updated context
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("fetch_%s", url.Loc))
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, "GET", url.Loc, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", gleanerAgent)
	req.Header.Set("Accept", "application/ld+json")

	resp, err := config.httpClient.Do(req)
	if err != nil {
		return err
	}
	span.AddEvent("http_response", trace.WithAttributes(attribute.KeyValue{Key: "status", Value: attribute.StringValue(resp.Status)}))

	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errormsg := fmt.Sprintf("failed to fetch %s, got status %s", url.Loc, resp.Status)
		log.Error(errormsg)
		// status makes jaeger mark as failed with red, whereas SetEvent just marks it with a message
		span.SetStatus(codes.Error, errormsg)
		config.nonFatalErrorChan <- UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
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
			config.nonFatalErrorChan <- urlErr
			return nil
		}
		return fmt.Errorf("failed to get JSON-LD from response: %w", err)
	}

	// To generate a hash we need to copy the response body
	itemHash, err := generateHashFilename(jsonld)
	if err != nil {
		return err
	}

	summonedPath := fmt.Sprintf("summoned/%s/%s", sitemapId, itemHash)

	exists, err := config.storageDestination.Exists(summonedPath)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// make sure the pointer itself is not nil and not empty
	if config.grpcClient != nil && *config.grpcClient != nil {
		triples, err := common.JsonldToNQ(string(jsonld), config.jsonLdProc, config.jsonLdOpt)
		if err != nil {
			return fmt.Errorf("failed to convert JSON-LD to N-Quads: %w", err)
		}
		err = validate_shacl(ctx, *config.grpcClient, triples)
		if err != nil {
			if urlErr, ok := err.(UrlCrawlError); ok {
				log.Errorf("SHACL validation failed for %s: %s", url.Loc, urlErr.Message)
				config.nonFatalErrorChan <- urlErr
				return nil
			}
			return fmt.Errorf("failed to validate shacl: %w", err)
		}
	}

	// Store from the buffered copy
	if err = config.storageDestination.Store(summonedPath, bytes.NewReader(jsonld)); err != nil {
		return err
	}

	if config.robots != nil && config.robots.CrawlDelay > 0 {
		log.Debug("sleeping for", config.robots.CrawlDelay)
		time.Sleep(config.robots.CrawlDelay)
	}

	return nil
}
