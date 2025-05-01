// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"nabu/internal/interfaces"
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
	require.Equal(t, "CUAHSI", id)

	badp := parts{Loc: "https://geoconnex.us"}
	id, err = badp.associatedID()
	require.Error(t, err)
	require.Equal(t, "", id)
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
		sitemap, err := NewSitemap(url)
		require.NoError(t, err)
		if len(sitemap.URL) == 0 {
			emptyMaps = append(emptyMaps, url)
		}
	}
	// the array of empty sitemap names should be empty, signifying there are no empty sitemaps
	assert.Len(t, emptyMaps, 0)
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
	tmpStore, err := interfaces.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	container, err := s3.NewDefaultMinioContainer()
	require.NoError(t, err)

	// get the sitemap index
	sitemapUrls, err := NewSitemapIndexHarvester("https://geoconnex.us/sitemap.xml")
	require.NoError(t, err)
	// just test the first for the sake of brevity
	sitemap, err := NewSitemap(sitemapUrls.GetUrlList()[0])
	require.NoError(t, err)

	errs := sitemap.SetStorageDestination(tmpStore).Harvest(10, "test")
	require.Empty(t, errs)

	errs = sitemap.SetStorageDestination(container.ClientWrapper).Harvest(1, "test")
	require.Empty(t, errs)
	numObjs, err := container.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Equal(t, numObjs, 3)

	require.True(t, gock.IsDone())
}
