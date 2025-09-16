// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"net/http"
	"testing"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/crawl/storage"
	"github.com/internetofwater/nabu/internal/synchronizer/s3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSitemapPartId(t *testing.T) {
	p := parts{Loc: "https://geoconnex.us/sitemap/CUAHSI/CUAHSI_HIS_GHCN_ids__0.xml"}
	id, err := p.associatedID()
	require.NoError(t, err)
	require.Equal(t, "CUAHSI_CUAHSI_HIS_GHCN_ids__0", id)

	p = parts{Loc: "https://geoconnex.us/sitemap/nhdplusv2/huc12pp/huc12pp__0.xml"}
	id, err = p.associatedID()
	require.NoError(t, err)
	require.Equal(t, "nhdplusv2_huc12pp_huc12pp__0", id)

	badp := parts{Loc: ""}
	id, err = badp.associatedID()
	require.Empty(t, id)
	require.Error(t, err)
}

// Test parsing the geoconnex sitemap index which contains links to other sitemaps
func TestParseSitemapIndex(t *testing.T) {

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap_index.xml",
		},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap.xml",
		},
	})

	indexHarvester, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml", mockedClient)
	require.NotEmpty(t, indexHarvester)
	assert.NoError(t, err)
	var emptyMaps []string

	for _, url := range indexHarvester.GetUrlList() {
		assert.NotEmpty(t, url)
		sitemap, err := NewSitemap(context.Background(), mockedClient, url)
		require.NoError(t, err)
		if len(sitemap.URL) == 0 {
			emptyMaps = append(emptyMaps, url)
		}
	}
	// the array of empty sitemap names should be empty, signifying there are no empty sitemaps
	assert.Empty(t, emptyMaps)
}

func TestHarvestNonExistantSource(t *testing.T) {

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{

		"https://geoconnex.us/sitemap.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap_index.xml",
		},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap.xml",
		},
	})

	indexHarvester, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml", mockedClient)
	assert.NoError(t, err)
	_, err = indexHarvester.HarvestSitemap(context.Background(), mockedClient, "does_not_exist")
	require.Error(t, err)
	require.ErrorContains(t, err, "not found in sitemap")
}

func TestHarvestSitemapIndex(t *testing.T) {

	// initialize storage types
	tmpStore, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	container, err := s3.NewDefaultMinioContainer()
	require.NoError(t, err)

	mockedClient := common.NewMockedClient(true, map[string]common.MockResponse{
		"https://geoconnex.us/sitemap.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap_index.xml",
		},
		"https://geoconnex.us/sitemap/iow/wqp/stations__5.xml": {
			StatusCode: 200,
			File:       "testdata/sitemap.xml",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C": {
			StatusCode:  200,
			File:        "testdata/1084.jsonld",
			ContentType: "application/ld+json",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2": {
			StatusCode:  200,
			File:        "testdata/1085.jsonld",
			ContentType: "application/ld+json",
		},
		"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A": {
			StatusCode:  200,
			File:        "testdata/1086.jsonld",
			ContentType: "application/ld+json",
		},
	})

	// get the sitemap index
	sitemapUrls, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml", mockedClient)
	require.NoError(t, err)
	// just test the first for the sake of brevity
	sitemap, err := NewSitemap(context.Background(), mockedClient, sitemapUrls.GetUrlList()[0])
	require.NoError(t, err)

	_, errs := sitemap.SetStorageDestination(tmpStore).Harvest(context.Background(), mockedClient, 10, "test", "", false, true)
	require.NoError(t, errs)

	_, errs = sitemap.SetStorageDestination(container.ClientWrapper).Harvest(context.Background(), mockedClient, 1, "test", "", false, true)
	require.NoError(t, errs)
	numObjs, err := container.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Equal(t, 3, numObjs)
}

func TestSitemapInsteadOfSitemapIndex(t *testing.T) {
	_, err := NewSitemapIndexHarvester("testdata/sitemap.xml", http.DefaultClient)
	require.Error(t, err)
	require.ErrorContains(t, err, "empty")
}
