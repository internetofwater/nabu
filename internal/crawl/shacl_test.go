// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"strings"
	"testing"

	"github.com/internetofwater/nabu/internal/protoBuild"
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

func TestNewShaclGrpcClientFromAddr(t *testing.T) {
	client, err := NewShaclGrpcClientFromAddr("0.0.0.0:50051")
	require.NoError(t, err)
	require.NotNil(t, client)
}
