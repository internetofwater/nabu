// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/internetofwater/nabu/internal/common/projectpath"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"github.com/internetofwater/nabu/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// a mock SHACL validator client for testing
type mockShaclValidatorClient struct{}

func (m *mockShaclValidatorClient) Validate(ctx context.Context, in *protoBuild.JsoldValidationRequest, opts ...grpc.CallOption) (*protoBuild.ValidationReply, error) {
	// for testing purposes, any jsonld missing the @context block is invalid
	// in the future you could extend this to do more complex validation
	if !strings.Contains(string(in.Jsonld), "@context") {
		return &protoBuild.ValidationReply{
			Valid:   false,
			Message: "invalid jsonld content",
		}, nil
	}
	return &protoBuild.ValidationReply{
		Valid:   true,
		Message: "valid",
	}, nil
}

var _ protoBuild.ShaclValidatorClient = &mockShaclValidatorClient{}

func TestNewGrpcShaclValidator(t *testing.T) {
	validator, err := NewGrpcShaclValidator("0.0.0.0:50051")
	require.NoError(t, err)
	require.Len(t, validator.clients, shaclGrpcConnections)
	require.NoError(t, validator.Close())
}

func TestNewGrpcShaclValidatorFromBlankAddr(t *testing.T) {
	_, err := NewGrpcShaclValidator("")
	require.Error(t, err)
}

func TestNewShaclValidatorFromConfig(t *testing.T) {
	validator, err := NewShaclValidatorFromConfig(false, "")
	require.NoError(t, err)
	require.Nil(t, validator, "no validator should be created if validation is not configured")

	validator, err = NewShaclValidatorFromConfig(true, "")
	require.NoError(t, err)
	require.IsType(t, &LocalShaclValidator{}, validator)

	validator, err = NewShaclValidatorFromConfig(false, "0.0.0.0:50051")
	require.NoError(t, err)
	require.IsType(t, &GrpcShaclValidator{}, validator)
	require.NoError(t, validator.Close())

	_, err = NewShaclValidatorFromConfig(true, "0.0.0.0:50051")
	require.Error(t, err, "local and grpc validation are mutually exclusive")
}

func TestGrpcShaclValidatorSpreadsRequests(t *testing.T) {
	first, second := &countingShaclValidatorClient{}, &countingShaclValidatorClient{}
	validator := NewGrpcShaclValidatorFromClients(first, second)
	for range 10 {
		_, _, err := validator.Validate(context.Background(), `{"@context": {}}`)
		require.NoError(t, err)
	}
	require.Equal(t, int64(5), first.calls.Load())
	require.Equal(t, int64(5), second.calls.Load())
}

type countingShaclValidatorClient struct {
	calls atomic.Int64
}

func (c *countingShaclValidatorClient) Validate(ctx context.Context, in *protoBuild.JsoldValidationRequest, opts ...grpc.CallOption) (*protoBuild.ValidationReply, error) {
	c.calls.Add(1)
	return &protoBuild.ValidationReply{Valid: true}, nil
}

func readShaclTestdata(t *testing.T, path ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{projectpath.Root, "shacl_validator", "testdata"}, path...)...))
	require.NoError(t, err)
	return string(data)
}

func TestLocalShaclValidator(t *testing.T) {
	validator, err := NewLocalShaclValidator()
	require.NoError(t, err)

	const url = "https://example.com/feature"

	require.NoError(t, validate_shacl(context.Background(), validator, url, readShaclTestdata(t, "valid", "rise.jsonld")))

	err = validate_shacl(context.Background(), validator, url, readShaclTestdata(t, "invalid", "no_geo.jsonld"))
	var shaclErr ShaclValidationFailureError
	require.ErrorAs(t, err, &shaclErr)
	require.Equal(t, url, shaclErr.Url)
	require.Contains(t, shaclErr.ShaclErrorMessage, "Places must include geometry in WKT format")

	// malformed jsonld is an error with validation itself rather than a shacl failure
	err = validate_shacl(context.Background(), validator, url, "{not json")
	require.Error(t, err)
	require.NotErrorAs(t, err, &shaclErr)

	err = validate_shacl(context.Background(), validator, url, "")
	var info pkg.ShaclInfo
	require.ErrorAs(t, err, &info)
	require.Equal(t, pkg.ShaclSkipped, info.ShaclStatus)
}

func TestLocalShaclValidatorConcurrent(t *testing.T) {
	validator, err := NewLocalShaclValidator()
	require.NoError(t, err)
	valid := readShaclTestdata(t, "valid", "rise.jsonld")
	invalid := readShaclTestdata(t, "invalid", "no_geo.jsonld")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			data, expected := valid, true
			if i%2 == 0 {
				data, expected = invalid, false
			}
			conforms, _, err := validator.Validate(context.Background(), data)
			assert.NoError(t, err)
			assert.Equal(t, expected, conforms)
		})
	}
	wg.Wait()
}
