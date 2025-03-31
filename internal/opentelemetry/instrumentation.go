package opentelemetry

import (
	"context"
	"log"
	"runtime"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace" // name this differently so it doesn't conflict with the tracer interface
	"go.opentelemetry.io/otel/trace"
)

// the global tracer instance that keeps track of client spans
var Tracer trace.Tracer

// Generate a new span and use the caller's function name as the span name
// This allows the span to be easily identified and prevents accidental mislabeling
func NewSpanWithContext() (trace.Span, context.Context) {
	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)
	name := fn.Name()
	ctx, span := Tracer.Start(context.Background(), name)
	return span, ctx
}

func SubSpanFromCtx(ctx context.Context, name string) (trace.Span, context.Context) {
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

func InitTracer() {
	ctx := context.Background()

	resource, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "nabu")),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint("127.0.0.1:4317"),
		otlptracegrpc.WithInsecure(),
	)

	otlpTraceExporter, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(otlpTraceExporter)
	filteringProcessor := &FilteringSpanProcessor{next: batchSpanProcessor}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(filteringProcessor), // Use filtered processor
		sdktrace.WithResource(resource),
	)

	otel.SetTracerProvider(tracerProvider)

	Tracer = tracerProvider.Tracer("nabu")
}
