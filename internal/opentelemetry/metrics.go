// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	metricInterfaces "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

var MeterProvider *metric.MeterProvider
var CrawlHistogram metricInterfaces.Float64Histogram
var FailureCounter metricInterfaces.Int64Counter

const DefaultMetricCollectorEndpoint = "localhost:5317"

func InitMetrics() {
	metricExporter, err := otlpmetricgrpc.New(
		context.Background(),
		otlpmetricgrpc.WithEndpoint(DefaultMetricCollectorEndpoint),
		otlpmetricgrpc.WithInsecure(), // Remove if using TLS
	)
	if err != nil {
		log.Fatal(err)
	}
	MeterProvider = metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			metric.WithInterval(10*time.Millisecond))),
	)

	// Register as global meter provider so that it can be used via otel.Meter
	// and accessed using otel.GetMeterProvider.
	otel.SetMeterProvider(MeterProvider)

	// Initialize the histogram
	CrawlHistogram, err = MeterProvider.Meter("gleaner").Float64Histogram("crawl_rate",
		metricInterfaces.WithDescription("Time to harvest a sitemap"),
	)
	if err != nil {
		log.Fatal(err)
	}

	FailureCounter, err = MeterProvider.Meter("gleaner").Int64Counter("total_sitemap_failures")
	if err != nil {
		log.Fatal(err)
	}
}

func SetFailuresForSitemap(sitemap string, failures int) {
	if MeterProvider == nil {
		return
	}

	FailureCounter.Add(context.Background(), 1,
		metricInterfaces.WithAttributes(
			attribute.String("sitemap", sitemap),
		),
	)
}

func SetHistogramValue(outputFoldername string, seconds float64) {
	if MeterProvider == nil {
		return
	}

	CrawlHistogram.Record(context.Background(), seconds, metricInterfaces.WithAttributes(
		attribute.String("output_foldername", outputFoldername)))
}
