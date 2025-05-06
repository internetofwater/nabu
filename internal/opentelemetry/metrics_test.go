// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {

	InitMetrics()

	defer func() {
		MeterProvider.ForceFlush(context.Background())
		MeterProvider.Shutdown(context.Background())
	}()

	SetFailuresForSitemap("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 404)

	counter, err := MeterProvider.Meter("gleaner").Int64Counter("failed_fetches")
	require.NoError(t, err)
	require.NotNil(t, counter)
}
