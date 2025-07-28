// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"bufio"
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/h2non/gock"
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

func TestHarvestOneSite(t *testing.T) {

	const dummy_domain = "http://google.com"

	const pingTheHashAndTheJsonLd = 2

	gock.New(dummy_domain).Reply(200).
		SetHeader("Content-Type", "application/ld+json").
		File("testdata/reference_feature.jsonld").
		Mock.Request().Times(pingTheHashAndTheJsonLd)

	defer gock.Off()

	url := URL{
		Loc: dummy_domain,
	}
	_, err := harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &SitemapHarvestConfig{
		httpClient:         &http.Client{},
		storageDestination: &storage.DiscardCrawlStorage{},
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

	rustProjRoot := filepath.Join(projectpath.Root, "shacl_validator_grpc")
	// run cargo run
	cwd, err := os.Getwd()
	require.NoError(t, err)
	err = os.Chdir(rustProjRoot)
	require.NoError(t, err)

	cmd := exec.Command(cargoPath, "run")
	stdout, err := cmd.StdoutPipe()
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
	}()
	select {
	case <-found:
		// Proceed
	case <-time.After(30 * time.Second):
		t.Fatal("Timed out waiting for gRPC server to start")
	}

	t.Run("valid jsonld", func(t *testing.T) {
		defer gock.Off()
		const dummy_domain = "http://google.com"
		gock.New(dummy_domain).Get("/").
			Reply(200).
			SetHeader("Content-Type", "application/ld+json").
			File("testdata/reference_feature.jsonld")

		gock.New(dummy_domain).Head("/").
			Reply(200).
			SetHeader("Content-Type", "application/ld+json")

		url := URL{
			Loc: dummy_domain,
		}
		conf, err := NewSitemapHarvestConfig(Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}}, true)
		require.NoError(t, err)
		// can't use the retriable http client with gock
		conf.httpClient = &http.Client{}
		_, err = harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		require.Empty(t, conf.nonFatalErrorChan)
	})
	t.Run("empty jsonld", func(t *testing.T) {
		defer gock.Off()
		const dummy_domain = "https://waterdata.usgs.gov"
		gock.New(dummy_domain).Get("/").
			Reply(200).
			SetHeader("Content-Type", "application/ld+json").
			File("testdata/emptyAsTriples.jsonld")

		gock.New(dummy_domain).Head("/").
			Reply(200).
			SetHeader("Content-Type", "application/ld+json")

		url := URL{
			Loc: dummy_domain,
		}
		conf, err := NewSitemapHarvestConfig(Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}}, true)

		require.NoError(t, err)
		// can't use the retriable http client with gock
		conf.httpClient = common.NewCrawlerClient()
		_, err = harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		close(conf.nonFatalErrorChan)
		require.Len(t, conf.nonFatalErrorChan, 1)
		for err := range conf.nonFatalErrorChan {
			require.Equal(t, pkg.ShaclSkipped, err.ShaclStatus)
		}
	})
	t.Run("nonconforming jsonld", func(t *testing.T) {
		defer gock.Off()
		const dummy_domain = "https://waterdata.usgs.gov"
		gock.New(dummy_domain).Get("/").
			Reply(200).
			SetHeader("Content-Type", "application/ld+json").
			File("testdata/nonconforming.jsonld")

		gock.New(dummy_domain).Head("/").
			Reply(200)
		url := URL{
			Loc: dummy_domain,
		}
		conf, err := NewSitemapHarvestConfig(Sitemap{URL: []URL{url}, storageDestination: &storage.DiscardCrawlStorage{}}, true)
		require.NoError(t, err)
		// can't use the retriable http client with gock
		conf.httpClient = &http.Client{}
		_, err = harvestOneSite(context.Background(), "DUMMY_SITEMAP", url, &conf)
		require.NoError(t, err)
		close(conf.nonFatalErrorChan)
		require.Len(t, conf.nonFatalErrorChan, 1)
		for err := range conf.nonFatalErrorChan {
			require.Equal(t, pkg.ShaclInvalid, err.ShaclStatus)
		}
	})
}
