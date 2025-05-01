// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package opentelemetry

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func TestInitTracer(t *testing.T) {
	require.Nil(t, Tracer)
	InitTracer("gleanerTest", "127.0.0.1:4317")
	require.NotNil(t, Tracer)
}

func TestCreateSpan(t *testing.T) {
	Tracer = nil
	InitTracer("gleanerTest", "127.0.0.1:4317")
	span, ctx := NewSpanAndContext()
	defer span.End()
	require.NotNil(t, span)
	require.True(t, span.SpanContext().IsValid())

	subspan, _ := SubSpanFromCtx(ctx)
	defer subspan.End()
	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	req, err := http.NewRequest("GET", "http://example.com", nil)
	require.NoError(t, err)
	_, err = client.Do(req)
	require.NoError(t, err)
}
