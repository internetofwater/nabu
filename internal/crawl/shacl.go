// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"fmt"

	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"github.com/internetofwater/nabu/pkg"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShaclValidationFailureError struct {
	ShaclErrorMessage string
	Url               string
}

func (e ShaclValidationFailureError) Error() string {
	return fmt.Sprintf("shacl validation failed for %s: %s", e.Url, e.ShaclErrorMessage)
}

// validate jsonld by sending them to the grpc server and checking the response
func validate_shacl(ctx context.Context, grpcClient protoBuild.ShaclValidatorClient, urlSource string, jsonldContent string) error {
	// no point in validating if there is no jsonld content; we don't want to be saving empty files
	if jsonldContent == "" {
		return pkg.ShaclInfo{ShaclStatus: pkg.ShaclSkipped, ShaclValidationMessage: "no jsonld to validate"}
	}
	ctx, grpcSubspan := opentelemetry.SubSpanFromCtxWithName(ctx, "grpc_shacl_validation")
	defer grpcSubspan.End()
	log.Tracef("validating jsonld of byte size %d", len(jsonldContent))
	reply, err := grpcClient.Validate(ctx, &protoBuild.JsoldValidationRequest{Jsonld: jsonldContent})
	if err != nil {
		return fmt.Errorf("failed sending validation request to gRPC server: %w", err)
	} else if !reply.Valid {
		grpcSubspan.SetStatus(codes.Error, reply.Message)
		return ShaclValidationFailureError{ShaclErrorMessage: reply.Message, Url: urlSource}
	} else {
		return nil
	}
}

// Given an address, return the gRPC client for SHACL validation
func NewShaclGrpcClientFromAddr(shaclAddress string) (protoBuild.ShaclValidatorClient, error) {
	// if the default string is provided, i.e. "" that means that the struct that
	// created it was also empty, so we skip creating the client
	if shaclAddress == "" {
		log.Warn("empty address passed to shacl client constructor. Skipping...")
		return nil, nil
	}

	// 32 megabytes is the current upperbound of the jsonld documents we will validate
	// beyond that is a sign that the document may be too large or incorrectly formatted
	thirtyTwoMB := 32 * 1024 * 1024
	conn, err := grpc.NewClient(shaclAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithMaxHeaderListSize(uint32(thirtyTwoMB)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}
	grpcClient := protoBuild.NewShaclValidatorClient(conn)
	return grpcClient, nil
}
