package opentelemetry

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func TestInitTracer(t *testing.T) {
	require.Nil(t, Tracer)
	InitTracer()
	require.NotNil(t, Tracer)
}

func TestCreateSpan(t *testing.T) {
	Tracer = nil
	InitTracer()
	span, ctx := NewSpanWithContext()
	defer span.End()
	require.NotNil(t, span)
	require.True(t, span.SpanContext().IsValid())

	subspan, _ := SubSpanFromCtx(ctx, "http get")
	defer subspan.End()
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	req, err := http.NewRequest("GET", "http://example.com", nil)
	require.NoError(t, err)
	_, err = client.Do(req)
	require.NoError(t, err)
}
