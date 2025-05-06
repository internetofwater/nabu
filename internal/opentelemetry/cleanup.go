// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"context"

	log "github.com/sirupsen/logrus"
)

// Shutdown any providers and flush any remaining spans
// This should be called when the top level application is shutting down
func Shutdown() {
	if TracerProvider != nil {
		err := TracerProvider.ForceFlush(context.Background())
		if err != nil {
			log.Errorf("Error flushing traces; is the collector for traces running?; %v", err)
		}
		err = TracerProvider.Shutdown(context.Background())
		if err != nil {
			log.Errorf("Error shutting down tracer provider: %v", err)
		}
	}

	if MeterProvider != nil {
		err := MeterProvider.ForceFlush(context.Background())
		if err != nil {
			log.Errorf("Error flushing metrics; Is the collector for metrics running?; %v", err)
		}
		err = MeterProvider.Shutdown(context.Background())
		if err != nil {
			log.Errorf("Error shutting down meter provider: %v", err)
		}
	}
}
