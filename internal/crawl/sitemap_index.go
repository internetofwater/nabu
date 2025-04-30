package crawl

import (
	"encoding/xml"
	"fmt"
	"nabu/internal/interfaces"
	"net/url"
	"strings"
	"time"

	sitemap "github.com/oxffaa/gopher-parse-sitemap"
	"golang.org/x/sync/errgroup"
)

// Index is a structure of <sitemapindex>
type Index struct {
	XMLName  xml.Name `xml:"sitemapindex"`
	Sitemaps []parts  `xml:"sitemap"`

	storageDestination interfaces.CrawlStorage `xml:"-"`
	concurrentSitemaps int                     `xml:"-"`
	sitemapWorkers     int                     `xml:"-"`
}

// parts is a structure of <sitemap> in <sitemapindex>
type parts struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod"`
}

func isUrl(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}

func NewSitemapIndexHarvester(sitemapRef string) (Index, error) {

	serializedSitemapIndex := Index{}

	if isUrl(sitemapRef) {
		return serializedSitemapIndex, sitemap.ParseIndexFromSite(sitemapRef, func(ie sitemap.IndexEntry) error {
			part := parts{}
			part.Loc = strings.TrimSpace(ie.GetLocation())
			part.LastMod = ie.GetLastModified().Format(time.RFC3339)
			serializedSitemapIndex.Sitemaps = append(serializedSitemapIndex.Sitemaps, part)
			return nil
		})
	} else {
		return serializedSitemapIndex, sitemap.ParseIndexFromFile(sitemapRef, func(ie sitemap.IndexEntry) error {
			part := parts{}
			part.Loc = strings.TrimSpace(ie.GetLocation())
			part.LastMod = ie.GetLastModified().Format(time.RFC3339)
			serializedSitemapIndex.Sitemaps = append(serializedSitemapIndex.Sitemaps, part)
			return nil
		})
	}

}

func (i Index) GetUrlList() []string {
	result := []string{}
	for _, part := range i.Sitemaps {
		result = append(result, part.Loc)
	}
	return result
}

// Harvest all the URLs in the sitemap
func (i Index) HarvestAll() error {

	var group errgroup.Group
	group.SetLimit(i.concurrentSitemaps)

	for _, part := range i.Sitemaps {
		part := part
		group.Go(func() error {
			sitemap, err := NewSitemap(part.Loc)
			if err != nil {
				return err
			}
			return sitemap.SetStorageDestination(i.storageDestination).Harvest(i.sitemapWorkers)
		})
	}

	return group.Wait()
}

// Harvest one particular sitemap
func (i Index) HarvestSitemap(sitemap string) error {

	for _, part := range i.Sitemaps {
		if part.Loc != sitemap {
			continue
		}
		sitemap, err := NewSitemap(part.Loc)
		if err != nil {
			return err
		}
		return sitemap.SetStorageDestination(i.storageDestination).Harvest(i.sitemapWorkers)
	}
	return fmt.Errorf("sitemap %s not found in sitemap", sitemap)
}

func (i Index) WithStorageDestination(storageDestination interfaces.CrawlStorage) Index {
	i.storageDestination = storageDestination
	return i
}

func (i Index) WithConcurrencyConfig(concurrentSitemaps int, sitemapWorkers int) Index {
	// Make sure concurrency is at least 1
	// otherwise go will block indefinitely
	if concurrentSitemaps < 1 {
		concurrentSitemaps = 1
	}
	if sitemapWorkers < 1 {
		sitemapWorkers = 1
	}

	i.concurrentSitemaps = concurrentSitemaps
	i.sitemapWorkers = sitemapWorkers
	return i
}
