package opentelemetry

import (
	"context"
	"log"
	"runtime"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace" // name this differently so it doesn't conflict with the tracer interface
	"go.opentelemetry.io/otel/trace"
)

func NewSpan() (context.Context, trace.Span) {
	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)
	return Tracer.Start(context.Background(), fn.Name())
}

// the global tracer instance that keeps track of client spans
var Tracer trace.Tracer

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

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(batchSpanProcessor),
		sdktrace.WithResource(resource),
	)

	otel.SetTracerProvider(tracerProvider)
	Tracer = tracerProvider.Tracer("nabu-tracer")
}
