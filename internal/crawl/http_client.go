// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/h2non/gock"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type RetriableCrawlerHttpClient struct {
	client  *http.Client
	retries int
	backoff time.Duration
}

type HttpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

func (r *RetriableCrawlerHttpClient) Do(req *http.Request) (*http.Response, error) {
	for retryNumber := range r.retries {
		resp, err := r.client.Do(req)

		totalTime := time.Duration(retryNumber+1) * r.backoff

		if err, ok := err.(net.Error); ok && err.Timeout() {
			log.Warnf("retrying after status %s from %s", resp.Status, req.URL.String())
			time.Sleep(totalTime)
			continue
		} else if err != nil {
			// if there is an error sending the request
			// i.e. it just can't connect over the network before
			// even reaching the target, we can just return the error
			return nil, err
		}
		if resp.StatusCode == 404 {
			return nil, fmt.Errorf("got a 404 from %s", req.URL.String())
		} else if resp.StatusCode > 400 {
			log.Warnf("retrying after status %s from %s", resp.Status, req.URL.String())
			time.Sleep(totalTime)
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("failed to get a response from %s after %d retries", req.URL.String(), r.retries)
}

func NewCrawlerHttpClient() *RetriableCrawlerHttpClient {

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
	if testing.Testing() {
		client.Transport = gock.DefaultTransport
	}
	return &RetriableCrawlerHttpClient{
		client:  client,
		retries: 3,
		backoff: 5 * time.Second,
	}
}
