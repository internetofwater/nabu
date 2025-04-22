package gleaner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Test parsing the geoconnex sitemap index which contains links to other sitemaps
func TestParseSitemapIndex(t *testing.T) {
	sitemapUrls, err := GetSitemapListFromIndex("https://geoconnex.us/sitemap.xml")
	require.NotEmpty(t, sitemapUrls)
	assert.NoError(t, err)
	var emptyMaps []string
	var group errgroup.Group
	group.SetLimit(20)

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

}

func TestHarvestSitemap(t *testing.T) {
	sitemap := Sitemap{URL: []URL{}}
	err := sitemap.Harvest(10)
	assert.NoError(t, err)
}
