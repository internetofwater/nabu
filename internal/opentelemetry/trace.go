// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"context"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace" // name this differently so it doesn't conflict with the tracer interface
	"go.opentelemetry.io/otel/trace"
)

const DefaultTracingEndpoint = "127.0.0.1:4317"

// the global tracer instance that keeps track of client spans
var Tracer trace.Tracer
var TracerProvider *sdktrace.TracerProvider

// Generate a new span and use the caller's function name as the span name
// This allows the span to be easily identified and prevents accidental mislabeling
func NewSpanAndContext() (trace.Span, context.Context) {
	if Tracer == nil {
		log.Fatal("Tracer is nil so cannot create span")
	}

	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)
	name := fn.Name()
	ctx, span := Tracer.Start(context.Background(), name)
	return span, ctx
}

func NewSpanAndContextWithName(name string) (trace.Span, context.Context) {
	if Tracer == nil {
		log.Fatal("Tracer is nil so cannot create span")
	}

	ctx, span := Tracer.Start(context.Background(), name)
	return span, ctx
}

func SubSpanFromCtx(ctx context.Context) (trace.Span, context.Context) {
	// If tracer is nil and we aren't using open telemetry, return a dummy
	// span that fulfills the interface but doesn't do anything
	if Tracer == nil {
		return trace.SpanFromContext(context.Background()), ctx
	}

	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)
	name := fn.Name()
	newCtx, span := Tracer.Start(ctx, name)
	return span, newCtx
}

func SubSpanFromCtxWithName(ctx context.Context, name string) (trace.Span, context.Context) {
	// If tracer is nil and we aren't using open telemetry, return a dummy
	// span that fulfills the interface but doesn't do anything
	if Tracer == nil {
		return trace.SpanFromContext(context.Background()), ctx
	}
	ctx, span := Tracer.Start(ctx, name)
	return span, ctx
}

// CustomSpanProcessor filters out Testcontainers spans
type FilteringSpanProcessor struct {
	next sdktrace.SpanProcessor
}

func (f *FilteringSpanProcessor) OnStart(parent context.Context, span sdktrace.ReadWriteSpan) {
	f.next.OnStart(parent, span)
}

func (f *FilteringSpanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	// Filter out Testcontainers spans based on attributes
	if shouldFilterOutSpan(span) {
		return // Skip processing
	}
	f.next.OnEnd(span)
}

func (f *FilteringSpanProcessor) Shutdown(ctx context.Context) error {
	return f.next.Shutdown(ctx)
}

func (f *FilteringSpanProcessor) ForceFlush(ctx context.Context) error {
	return f.next.ForceFlush(ctx)
}

func shouldFilterOutSpan(span sdktrace.ReadOnlySpan) bool {
	attrs := span.Attributes()
	for _, attr := range attrs {
		if attr.Key == "http.url" {
			if strings.Contains(attr.Value.AsString(), "/containers/") {
				return true // Ignore Testcontainers requests
			}
		}
		if attr.Key == "user_agent.original" && strings.Contains(attr.Value.AsString(), "tc-go") {
			return true // Ignore requests from Testcontainers' user agent
		}
	}
	return false
}

func InitTracer(serviceName string, endpoint string) {
	ctx := context.Background()

	resource, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)

	otlpTraceExporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(otlpTraceExporter)
	filteringProcessor := &FilteringSpanProcessor{next: batchSpanProcessor}

	TracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(filteringProcessor), // Use filtered processor
		sdktrace.WithResource(resource),
	)

	otel.SetTracerProvider(TracerProvider)

	Tracer = TracerProvider.Tracer(serviceName) // Set the global tracer

	log.Infof("OpenTelemetry Tracer initialized, sending traces to %s", endpoint)
}

