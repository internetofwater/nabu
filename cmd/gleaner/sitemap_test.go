package gleaner

import (
	"testing"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Test parsing the geoconnex sitemap index which contains links to other sitemaps
func TestParseSitemapIndex(t *testing.T) {

	defer gock.Off()
	defer gock.DisableNetworking()

	gock.New("https://geoconnex.us/sitemap.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	sitemapUrls, err := GetSitemapListFromIndex("https://geoconnex.us/sitemap.xml")
	require.NotEmpty(t, sitemapUrls)
	assert.NoError(t, err)
	var emptyMaps []string
	var group errgroup.Group
	group.SetLimit(20)
	gock.EnableNetworking()

	for _, url := range sitemapUrls {
		assert.NotEmpty(t, url)
		url := url // capture loop var
		group.Go(func() error {
			sitemap, err := NewSitemap(url)
			if err != nil {
				return err
			}
			if len(sitemap.URL) == 0 {
				emptyMaps = append(emptyMaps, url)
			}
			return nil
		})
	}

	assert.NoError(t, group.Wait())
	// the array of empty sitemap names should be empty, signifying there are no empty sitemaps
	assert.Len(t, emptyMaps, 0)
	require.True(t, gock.IsDone())

}

func TestHarvestSitemap(t *testing.T) {

	t.Run("test one url", func(t *testing.T) {
		sitemap := Sitemap{URL: []URL{{Loc: "https://waterdata.usgs.gov/monitoring-location/354820117401201"}}}
		err := sitemap.WithStorageType(DiscardCrawlStorage{}).Harvest(10)
		for _, err := range err {
			require.NoError(t, err)
		}
	})

	t.Run("test multiple urls", func(t *testing.T) {
		sitemapUrls, err := GetSitemapListFromIndex("https://geoconnex.us/sitemap.xml")
		require.NoError(t, err)
		sitemap, err := NewSitemap(sitemapUrls[0])
		require.NoError(t, err)
		_ = sitemap.WithStorageType(DiscardCrawlStorage{}).Harvest(10)
	})
}
