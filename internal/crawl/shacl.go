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
)

// validate triples by sending them to the grpc server and checking the response
func validate_shacl(ctx context.Context, grpcClient protoBuild.ShaclValidatorClient, triples string) error {
	// no point in validating if there are no triples; we don't want to be saving empty files
	if triples == "" {
		return pkg.UrlCrawlError{ShaclStatus: pkg.ShaclSkipped, ShaclErrorMessage: "no triples to validate"}
	}
	ctx, grpcSubspan := opentelemetry.SubSpanFromCtxWithName(ctx, "grpc_shacl_validation")
	log.Debugf("validating triples of byte size %d", len(triples))
	reply, err := grpcClient.Validate(ctx, &protoBuild.TurtleValidationRequest{Triples: triples})
	if err != nil {
		grpcSubspan.End()
		return fmt.Errorf("failed sending validation request to gRPC server: %w", err)
	} else if !reply.Valid {
		grpcSubspan.SetStatus(codes.Error, reply.Message)
		grpcSubspan.End()
		var shaclStatus pkg.ShaclStatus
		if reply.Valid {
			shaclStatus = pkg.ShaclValid
		} else {
			shaclStatus = pkg.ShaclInvalid
		}
		return pkg.UrlCrawlError{ShaclStatus: shaclStatus, ShaclErrorMessage: reply.Message}
	} else {
		grpcSubspan.End()
		return nil
	}
}
