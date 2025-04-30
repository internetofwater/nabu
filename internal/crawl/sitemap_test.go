// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"nabu/internal/interfaces"
	"testing"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/require"
)

func TestHarvestSitemap(t *testing.T) {
	defer gock.Off()

	gock.New("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml").
		Reply(200).
		File("testdata/sitemap.xml")

	sitemap, err := NewSitemap("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml")
	require.NoError(t, err)
	errs := sitemap.SetStorageDestination(interfaces.DiscardCrawlStorage{}).Harvest(10)
	require.Empty(t, errs)
}
