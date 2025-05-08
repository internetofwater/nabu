// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"nabu/internal/crawl/storage"
	"nabu/internal/synchronizer/s3"
	"testing"

	"github.com/h2non/gock"
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

	defer gock.Off()

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	indexHarvester, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml")
	require.NotEmpty(t, indexHarvester)
	assert.NoError(t, err)
	var emptyMaps []string

	for _, url := range indexHarvester.GetUrlList() {
		assert.NotEmpty(t, url)
		sitemap, err := NewSitemap(context.Background(), url)
		require.NoError(t, err)
		if len(sitemap.URL) == 0 {
			emptyMaps = append(emptyMaps, url)
		}
	}
	// the array of empty sitemap names should be empty, signifying there are no empty sitemaps
	assert.Empty(t, emptyMaps)
}

func TestHarvestNonExistantSource(t *testing.T) {

	defer gock.Off()

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	indexHarvester, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml")
	assert.NoError(t, err)
	_, err = indexHarvester.HarvestSitemap(context.Background(), "does_not_exist")
	require.Error(t, err)
	require.ErrorContains(t, err, "not found in sitemap")
}

func TestHarvestSitemapIndex(t *testing.T) {
	// setup mocks
	defer gock.Off()
	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	// initialize storage types
	tmpStore, err := storage.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	container, err := s3.NewDefaultMinioContainer()
	require.NoError(t, err)

	// get the sitemap index
	sitemapUrls, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml")
	require.NoError(t, err)
	// just test the first for the sake of brevity
	sitemap, err := NewSitemap(context.Background(), sitemapUrls.GetUrlList()[0])
	require.NoError(t, err)

	_, errs := sitemap.SetStorageDestination(tmpStore).Harvest(context.Background(), 10, "test")
	require.NoError(t, errs)

	_, errs = sitemap.SetStorageDestination(container.ClientWrapper).Harvest(context.Background(), 1, "test")
	require.NoError(t, errs)
	numObjs, err := container.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Equal(t, 3, numObjs)

	require.True(t, gock.IsDone())
}

func TestSitemapInsteadOfSitemapIndex(t *testing.T) {
	_, err := NewSitemapIndexHarvester("testdata/sitemap.xml")
	require.Error(t, err)
	require.ErrorContains(t, err, "empty")
}
