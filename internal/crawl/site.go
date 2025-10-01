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

	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
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
			return nil, pkg.UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
		}
	}
	return body, nil
}

// Get the hash of the remote jsonld by using the Content-Digest header
// This gets us metadata about the file without needing to download it fully
func getRemoteJsonldHash(url string, client *http.Client) (string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", gleanerAgent)
	req.Header.Set("Want-Content-Digest", "sha256")
	req.Header.Set("Accept", "application/ld+json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return "", nil
	}
	hash := resp.Header.Get("content-digest")

	// make sure we get just the hash value
	// not the metadata about the hash itself
	trimmed := strings.TrimPrefix(hash, "sha256=")
	trimmed = strings.TrimPrefix(trimmed, "sha256-")
	trimmed = strings.TrimSpace(trimmed)

	return trimmed, nil
}

type harvestResult struct {
	pathInStorage string
	serverHadHash bool
	warning       pkg.ShaclInfo
	nonFatalError pkg.UrlCrawlError
}

// Crawl and download a single URL
func harvestOneSite(ctx context.Context, sitemapId string, url URL, config *SitemapHarvestConfig) (harvestResult, error) {
	if sitemapId == "" {
		return harvestResult{}, fmt.Errorf("no sitemap id specified. Must be set for identifying the sitemap with a human readable name")
	}

	// Create a new span for each URL and propagate the updated context
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("fetch_%s", url.Loc))
	defer span.End()

	var hash string
	var expectedLocationInStorage string

	result_metadata := harvestResult{}

	if config.checkExistenceBeforeCrawl.Load() {
		hash, err := getRemoteJsonldHash(url.Loc, config.httpClient)
		if err != nil {
			return result_metadata, fmt.Errorf("failed to get hash for %s: %w", url.Loc, err)
		}
		var expectedLocationInStorage string
		if hash != "" {
			result_metadata.serverHadHash = true
			expectedLocationInStorage = "summoned/" + sitemapId + "/" + hash + ".jsonld"
			exists, err := config.storageDestination.Exists(expectedLocationInStorage)
			if err != nil {
				return result_metadata, err
			}
			if exists {
				log.Infof("skipping %s because it already exists in %s", url.Loc, expectedLocationInStorage)
				result_metadata.pathInStorage = expectedLocationInStorage
				return result_metadata, nil
			}
			log.Tracef("%s does not exist in the bucket", expectedLocationInStorage)

		} else {
			log.Tracef("%s has no associated hash", url.Loc)
		}
	}

	log.Tracef("fetching %s", url.Loc)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.Loc, nil)
	if err != nil {
		return result_metadata, err
	}
	req.Header.Set("User-Agent", gleanerAgent)
	req.Header.Set("Accept", "application/ld+json")

	resp, err := config.httpClient.Do(req)
	if err != nil {
		return result_metadata, err
	}
	span.AddEvent("http_response", trace.WithAttributes(attribute.KeyValue{Key: "status", Value: attribute.StringValue(resp.Status)}))

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		errormsg := fmt.Sprintf("failed to fetch %s, got status %s", url.Loc, resp.Status)
		log.Error(errormsg)
		// status makes jaeger mark as failed with red, whereas SetEvent just marks it with a message
		span.SetStatus(codes.Error, errormsg)
		result_metadata.nonFatalError = pkg.UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
		return result_metadata, nil
	}

	rawbytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return result_metadata, fmt.Errorf("failed to read response body: %w", err)
	}

	jsonld, err := getJSONLD(resp, url, rawbytes)
	if err != nil {
		// If it's a UrlCrawlError, store it for stats
		// put don't return it, since it is non fatal
		if urlErr, ok := err.(pkg.UrlCrawlError); ok {
			span.SetStatus(codes.Error, urlErr.Message)
			result_metadata.nonFatalError = urlErr
			return result_metadata, nil
		}
		return result_metadata, fmt.Errorf("failed to get JSON-LD from response: %w", err)
	}

	// To generate a hash we need to copy the response body
	itemHash := generateHashFilename(rawbytes)

	summonedPath := fmt.Sprintf("summoned/%s/%s", sitemapId, itemHash)

	if hash != "" && expectedLocationInStorage != "" {
		result_metadata.serverHadHash = true
		if summonedPath != expectedLocationInStorage {
			log.Fatalf("hashes appear to be different for %s \n %s", summonedPath, expectedLocationInStorage)
			return result_metadata, fmt.Errorf("summonedPath %s and whereItWouldBeInBucket %s are different", summonedPath, expectedLocationInStorage)
		}
	}

	// make sure the pointer itself is not nil and not empty
	if config.grpcClient != nil && *config.grpcClient != nil {
		err = validate_shacl(ctx, *config.grpcClient, url.Loc, string(jsonld))
		if err != nil {
			if shaclErr, ok := err.(ShaclValidationFailureError); ok {
				log.Errorf("Failure for %s: %s", url.Loc, shaclErr.ShaclErrorMessage)
				result_metadata.warning = pkg.ShaclInfo{
					ShaclStatus:            pkg.ShaclInvalid,
					ShaclValidationMessage: shaclErr.ShaclErrorMessage,
				}

				// we don't always return here because it is non fatal
				// and not all integrations may be compliant with our shacl shapes yet;
				// For the time being, it is better to harvest and then have the integrator fix it
				// after the fact; in the future there could be a strict
				// validation mode wherein we fail fast upon shacl non-compliance
				// however, we do allow a flag to exit and strictly fail
				if config.exitOnShaclFailure {
					log.Debugf("Returning early on shacl failure for %s", url.Loc)
					return result_metadata, nil
				}
			} else {
				return result_metadata, fmt.Errorf("failed to communicate with shacl validation service: %w", err)
			}
		}
	}

	// Store from the buffered copy
	if err = config.storageDestination.Store(summonedPath, bytes.NewReader(jsonld)); err != nil {
		return result_metadata, err
	}

	if config.robots != nil && config.robots.CrawlDelay > 0 {
		log.Debug("sleeping for", config.robots.CrawlDelay)
		time.Sleep(config.robots.CrawlDelay)
	}

	return result_metadata, nil
}
