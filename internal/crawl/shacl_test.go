// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"

	"github.com/internetofwater/nabu/internal/protoBuild"
	"google.golang.org/grpc"
)

type mockShaclValidatorClient struct{}

func (m *mockShaclValidatorClient) Validate(ctx context.Context, in *protoBuild.JsoldValidationRequest, opts ...grpc.CallOption) (*protoBuild.ValidationReply, error) {
	// for testing purposes, let's say any jsonld containing "invalid" is invalid
	if string(in.Jsonld) == "invalid" {
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
