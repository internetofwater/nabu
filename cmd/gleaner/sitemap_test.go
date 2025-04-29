package gleaner

import (
	"testing"

	crawl "nabu/internal/crawl"
	"nabu/internal/synchronizer/s3"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test parsing the geoconnex sitemap index which contains links to other sitemaps
func TestParseSitemapIndex(t *testing.T) {

	defer gock.Off()

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	sitemapUrls, err := GetSitemapListFromIndex("https://geoconnex.us/sitemap.xml")
	require.NotEmpty(t, sitemapUrls)
	assert.NoError(t, err)
	var emptyMaps []string

	for _, url := range sitemapUrls {
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

func TestHarvestSitemap(t *testing.T) {
	// setup mocks
	defer gock.Off()
	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap_index.xml")

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	// initialize storage types
	tmpStore, err := crawl.NewLocalTempFSCrawlStorage()
	require.NoError(t, err)
	container, err := s3.NewDefaultMinioContainer()
	require.NoError(t, err)

	// get the sitemap index
	sitemapUrls, err := GetSitemapListFromIndex("https://geoconnex.us/sitemap.xml")
	require.NoError(t, err)
	// just test the first for the sake of brevity
	sitemap, err := NewSitemap(sitemapUrls[0])
	require.NoError(t, err)

	errs := sitemap.SetStorageDestination(tmpStore).Harvest(10)
	require.Empty(t, errs)

	errs = sitemap.SetStorageDestination(container.ClientWrapper).Harvest(1)
	require.Empty(t, errs)
	numObjs, err := container.ClientWrapper.NumberOfMatchingObjects([]string{""})
	require.NoError(t, err)
	require.Equal(t, numObjs, 3)

	require.True(t, gock.IsDone())
}
