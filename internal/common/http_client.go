// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type MockResponse struct {
	file       string
	body       string
	statusCode int
}

type MockTransport struct {
	transport http.RoundTripper
	urlToFile map[string]MockResponse
}

// If the req url is in the map, return a mock response from the associated file
func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {

	full_url := req.URL.String()

	if (m.urlToFile[full_url] != MockResponse{}) {
		associatedMock, ok := m.urlToFile[full_url]

		if ok && associatedMock.body != "" {
			return &http.Response{
				StatusCode: associatedMock.statusCode,
				Body:       io.NopCloser(strings.NewReader(associatedMock.body)),
				Header:     http.Header{},
			}, nil
		} else {
			mockedContent, err := os.Open(associatedMock.file)
			if mockedContent == nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: associatedMock.statusCode,
				Body:       mockedContent,
				Header:     http.Header{},
			}, nil
		}

	}
	return m.transport.RoundTrip(req)
}

func NewMockedClient(urlToMock map[string]MockResponse) *http.Client {

	newLongLivedHttpTransport := newLongLivedHttpTransport()

	transport := &MockTransport{
		transport: newLongLivedHttpTransport,
		urlToFile: urlToMock,
	}

	return newClientFromRoundTrip(transport)
}

// RetryTransport implements retries and exponential backoff at the transport level
type RetryTransport struct {
	Base    http.RoundTripper
	Retries int
	Backoff time.Duration
}

func (t *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error

	for i := 0; i < t.Retries; i++ {
		resp, err := t.Base.RoundTrip(req)

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Warnf("retrying after timeout on %s (attempt %d)", req.URL.String(), i+1)
				time.Sleep(time.Duration(i+1) * t.Backoff)
				lastErr = err
				continue
			}
			return nil, err
		}

		if resp.StatusCode == 404 {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("got a 404 from %s", req.URL.String())
		} else if resp.StatusCode >= 500 {
			log.Warnf("retrying after server error %d from %s (attempt %d)", resp.StatusCode, req.URL.String(), i+1)
			_ = resp.Body.Close()
			time.Sleep(time.Duration(i+1) * t.Backoff)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("failed to get a successful response from %s after %d retries: %v", req.URL.String(), t.Retries, lastErr)
}

// An http transport optimized for long-lived connections
func newLongLivedHttpTransport() http.RoundTripper {
	return &http.Transport{
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   0,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			span := trace.SpanFromContext(ctx)
			if span != nil {
				span.AddEvent("HTTP connection")
			}
			return net.DialTimeout(network, addr, 30*time.Second)
		},
	}
}

// An http client optimized for long lived crawler requests and setting otel
func newClientFromRoundTrip(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			span := trace.SpanFromContext(req.Context())
			if span != nil {
				span.AddEvent("HTTP redirect")
			}
			return nil
		},
	}
}

func NewCrawlerClient() *http.Client {
	newLongLivedHttpTransport := newLongLivedHttpTransport()

	// We embed the long lived transport in the retry transport
	// so that we can retry these long lived connections
	crawlerTransport := &RetryTransport{
		Base:    newLongLivedHttpTransport,
		Retries: 3,
		Backoff: 2 * time.Second,
	}

	return newClientFromRoundTrip(crawlerTransport)
}
