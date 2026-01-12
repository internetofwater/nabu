// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	hashchecks "github.com/internetofwater/nabu/internal/crawl/hash_checks"
	"github.com/internetofwater/nabu/internal/crawl/url_info"
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
func getJSONLD(resp *http.Response, url url_info.URL, body []byte) ([]byte, error) {
	mime := resp.Header.Get("Content-Type")
	if strings.Contains(mime, "application/ld+json") {
		return body, nil
	} else if strings.Contains(mime, "text/html") {
		jsonldString, err := GetJsonLDFromHTML(body)
		if err != nil {
			log.Errorf("failed to parse jsonld within the html for %s", url.Loc)
			return nil, pkg.UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: err.Error()}
		}
		if jsonldString == "" || jsonldString == "{}" {
			log.Errorf("empty jsonld string '%s' found within the html for %s", jsonldString, url.Loc)
		}
		return []byte(jsonldString), nil
	}
	errormsg := fmt.Sprintf("got wrong file type %s for %s", mime, url.Loc)
	log.Error(errormsg)
	return nil, pkg.UrlCrawlError{Url: url.Loc, Status: resp.StatusCode, Message: errormsg}
}

// the metadata for a single url harvest
type harvestResult struct {
	pathInStorage string
	serverHadHash bool
	warning       pkg.ShaclInfo
	nonFatalError pkg.UrlCrawlError
}

// Crawl and download a single pid
func harvestOnePID(ctx context.Context, sitemapId string, url url_info.URL, config *SitemapHarvestConfig) (harvestResult, error) {
	if sitemapId == "" {
		return harvestResult{}, fmt.Errorf("no sitemap id specified. Must be set for identifying the sitemap with a human readable name")
	}
	if url.Base64Loc == "" {
		return harvestResult{}, fmt.Errorf("no base64 loc specified for %s", url.Loc)
	}

	// Create a new span for each URL and propagate the updated context
	ctx, span := opentelemetry.SubSpanFromCtxWithName(ctx, fmt.Sprintf("fetch_%s", url.Loc))
	defer span.End()

	var hash string
	var expectedLocationInStorage string

	result_metadata := harvestResult{}

	if config.checkExistenceBeforeCrawl.Load() {
		result, err := hashchecks.NewHashChecker(config.httpClient, config.storageDestination).
			CheckIfAlreadyExists(url, sitemapId)
		var nonFatalError pkg.UrlCrawlError
		if errors.As(err, &nonFatalError) {
			result_metadata.nonFatalError = nonFatalError
			return result_metadata, nil
		}
		if err != nil {
			return result_metadata, fmt.Errorf("got fatal error when checking if %s already exists: %w", url.Loc, err)
		}
		result_metadata.serverHadHash = result.ServerProvidedHash
		result_metadata.pathInStorage = result.PathInStorage
		if result.FileAlreadyExists {
			return result_metadata, nil
		}
	}

	log.Tracef("fetching %s", url.Loc)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.Loc, nil)
	if err != nil {
		return result_metadata, fmt.Errorf("failed to create http request for %s: %w", url.Loc, err)
	}
	req.Header.Set("User-Agent", common.HarvestAgent)
	req.Header.Set("Accept", "application/ld+json")

	resp, err := config.httpClient.Do(req)
	if err != nil {
		var maxErr *common.MaxRetryError
		if errors.As(err, &maxErr) || errors.Is(err, context.DeadlineExceeded) {
			result_metadata.nonFatalError = pkg.UrlCrawlError{Url: url.Loc, Message: err.Error()}
			return result_metadata, nil
		}
		return result_metadata, fmt.Errorf("got fatal error of type %s when fetching %s: %w", reflect.TypeOf(err).String(), url.Loc, err)
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
		return result_metadata, fmt.Errorf("failed to read response body for %s: %w", url.Loc, err)
	}

	jsonld, err := getJSONLD(resp, url, rawbytes)
	if err != nil {
		// If it's a UrlCrawlError, store it for stats
		// put don't return it, since it is non fatal
		if urlErr, ok := err.(pkg.UrlCrawlError); ok {
			span.SetStatus(codes.Error, urlErr.Message)
			result_metadata.nonFatalError = urlErr
			result_metadata.nonFatalError.Status = resp.StatusCode
			result_metadata.nonFatalError.Message = urlErr.Message
			return result_metadata, nil
		}
		return result_metadata, fmt.Errorf("failed to get JSON-LD from response: %w", err)
	}

	summonedPath, err := urlToStoragePath(sitemapId, url)
	if err != nil {
		return result_metadata, fmt.Errorf("failed to get storage path: %w", err)
	}
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
				result_metadata.warning = pkg.ShaclInfo{
					ShaclStatus:            pkg.ShaclInvalid,
					ShaclValidationMessage: shaclErr.ShaclErrorMessage,
					Url:                    url.Loc,
				}

				// we don't always return here because it is non fatal
				// and not all integrations may be compliant with our shacl shapes yet;
				// For the time being, it is better to harvest and then have the integrator fix it
				// after the fact; in the future there could be a strict
				// validation mode wherein we fail fast upon shacl non-compliance
				// however, we do allow a flag to exit and strictly fail
				if config.exitOnShaclFailure {
					log.Errorf("Returning early on shacl failure for %s with message %s", url.Loc, shaclErr.ShaclErrorMessage)
					return result_metadata, fmt.Errorf("exiting early for %s with shacl failure %s", url.Loc, shaclErr.ShaclErrorMessage)
				}
			} else {
				return result_metadata, fmt.Errorf("failed to communicate with shacl validation service when harvesting %s: %w", url.Loc, err)
			}
		}
	}

	// Store from the buffered copy
	if err = config.storageDestination.StoreWithHash(summonedPath, bytes.NewReader(jsonld), len(jsonld)); err != nil {
		return result_metadata, err
	}

	if config.robots != nil && config.robots.CrawlDelay > 0 {
		log.Debug("sleeping for", config.robots.CrawlDelay)
		time.Sleep(config.robots.CrawlDelay)
	}
	result_metadata.pathInStorage = summonedPath
	return result_metadata, nil
}
