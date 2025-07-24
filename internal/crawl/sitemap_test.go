// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"testing"

	"github.com/internetofwater/nabu/internal/crawl/storage"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
)

func TestHarvestSitemap(t *testing.T) {
	defer gock.Off()
	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	urls := []string{
		"https://geoconnex.us/iow/wqp/BPMWQX-1084-WR-CC01C",
		"https://geoconnex.us/iow/wqp/BPMWQX-1085-WR-CC01C2",
		"https://geoconnex.us/iow/wqp/BPMWQX-1086-WR-CC02A",
	}

	for _, url := range urls {
		gock.New(url).Get("/").
			Reply(200).
			File("testdata/reference_feature.jsonld")

		gock.New(url).Head("/").
			Reply(200)
	}

	sitemap, err := NewSitemap(context.Background(), "https://geoconnex.us/sitemap/iow/wqp/stations__5.xml")
	require.NoError(t, err)
	_, errs := sitemap.SetStorageDestination(storage.DiscardCrawlStorage{}).Harvest(context.Background(), 10, "test", false, false)
	require.NoError(t, errs)
}
