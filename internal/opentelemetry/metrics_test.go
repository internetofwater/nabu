// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {

	InitMetrics()

	defer func() {
		err := MeterProvider.ForceFlush(context.Background())
		if err != nil {
			log.Errorf("Error flushing metrics; Is the collector for metrics running?; %v", err)
		}
		err = MeterProvider.Shutdown(context.Background())
		if err != nil {
			log.Errorf("Error shutting down meter provider: %v", err)
		}
	}()

	SetFailuresForSitemap("https://geoconnex.us/sitemap/iow/wqp/stations__5.xml", 404)

	counter, err := MeterProvider.Meter("gleaner").Int64Counter("failed_fetches")
	require.NoError(t, err)
	require.NotNil(t, counter)
}
