// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	common "github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/common/projectpath"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/pkg"
	"github.com/stretchr/testify/require"
)

func TestGetJsonLDWithBadMimetype(t *testing.T) {

	resp := &http.Response{}
	resp.Header = http.Header{
		"Content-Type": []string{"text/DUMMY"},
	}
	_, err := getJSONLD(resp, URL{}, nil)
	require.ErrorAs(t, err, &pkg.UrlCrawlError{})

}

func TestTimeout(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			File:    "testdata/reference_feature.jsonld",
			Timeout: true,
		},
	})

	url := URL{
		Loc: dummy_domain,
	}
	check := atomic.Bool{}
	check.Store(false)
	report, err := harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
	require.Equal(t, 0, report.nonFatalError.Status)
	require.Contains(t, report.nonFatalError.Message, "timeout")
}

func TestHarvestOneSite(t *testing.T) {

	const dummy_domain = "http://google.com"

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		dummy_domain: {
			StatusCode:  200,
			File:        "testdata/reference_feature.jsonld",
			ContentType: "application/ld+json",
		},
	})

	url := URL{
		Loc: dummy_domain,
	}
	check := atomic.Bool{}
	check.Store(true)
	_, err := harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:                mockedClient,
		storageDestination:        &storage.DiscardCrawlStorage{},
		checkExistenceBeforeCrawl: &check,
	})
	require.NoError(t, err)
}

func TestHarvestWithShaclValidation(t *testing.T) {

	// if rust is installed just skip this since it is a non essential test
	// you don't have to run with grpc/shacl validation
	cargoPath, err := exec.LookPath("cargo")
	if err != nil {
		t.Skip("cargo not installed")
	} else if os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("skipping check in github actions; cargo build takes too long in ci")
	} else {
		t.Logf("cargo found at %s", cargoPath)
	}

	rustProjRoot := filepath.Join(projectpath.Root, "shacl_validator", "shacl_validator_grpc_rs")
	// run cargo run
	cwd, err := os.Getwd()
	require.NoError(t, err)
	err = os.Chdir(rustProjRoot)
	require.NoError(t, err)

	cmd := exec.Command(cargoPath, "run")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	err = cmd.Start()
	require.NoError(t, err)
	defer func() {
		_ = cmd.Process.Kill()
	}()
	//  restore cwd
	err = os.Chdir(cwd)
	require.NoError(t, err)

	// Wait for "Starting gRPC server" on stdout
	found := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Starting gRPC server") {
				close(found)
				return
			}
		}
		if err := scanner.Err(); err != nil {
			t.Error(err)
		}
		errContent, err := io.ReadAll(stderr)
		if err != nil {
			t.Error(err)
		}
		if len(errContent) > 0 {
			t.Error(string(errContent))
		}
	}()
	select {
	case <-found:
		// Proceed
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for gRPC server to start; the server may be failing to start due to a port conflict")
	}

	t.Run("valid jsonld", func(t *testing.T) {
		const dummy_domain = "http://google.com"

		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/reference_feature.jsonld",
				ContentType: "application/ld+json",
			},
		})

		url := URL{
			Loc: dummy_domain,
		}
		sitemap := Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}
		conf, err := NewSitemapHarvestConfig(mockedClient,
			&sitemap,
			"0.0.0.0:50051", false, false)
		require.NoError(t, err)
		_, err = harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
	})
	t.Run("empty jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"

		url := URL{
			Loc: dummy_domain,
		}

		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode: 200,
				File:       "testdata/emptyAsTriples.jsonld",
			},
		})
		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, "0.0.0.0:50051", false, false)

		require.NoError(t, err)
		report, err := harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		require.Empty(t, report.warning.ShaclStatus)
	})
	t.Run("nonconforming jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"
		url := URL{
			Loc: dummy_domain,
		}
		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/nonconforming.jsonld",
				ContentType: "application/ld+json",
			},
		})

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, "0.0.0.0:50051", false, false)
		require.NoError(t, err)
		report, err := harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		require.Equal(t, pkg.ShaclInvalid, report.warning.ShaclStatus)
	})

	t.Run("strict mode exits early on bad jsonld", func(t *testing.T) {
		const dummy_domain = "https://waterdata.usgs.gov"
		url := URL{
			Loc: dummy_domain,
		}
		mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
			dummy_domain: {
				StatusCode:  200,
				File:        "testdata/nonconforming.jsonld",
				ContentType: "application/ld+json",
			},
		})

		conf, err := NewSitemapHarvestConfig(mockedClient, &Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}, workers: 10}, "0.0.0.0:50051", true, true)
		require.NoError(t, err)
		_, err = harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.Error(t, err)
	})
}
