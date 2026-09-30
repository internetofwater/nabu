// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/internal/protoBuild"
	"github.com/internetofwater/nabu/pkg"
	shacl_validator "github.com/internetofwater/nabu/shacl_validator/shacl_validator_go"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// the number of separate gRPC connections to the SHACL validator;
// a multi-process validator balances by connection, so a single
// connection would send every request to the same process
const shaclGrpcConnections = 64

type ShaclValidationFailureError struct {
	ShaclErrorMessage string
	Url               string
}

func (e ShaclValidationFailureError) Error() string {
	return fmt.Sprintf("shacl validation failed for %s: %s", e.Url, e.ShaclErrorMessage)
}

// ShaclValidator validates jsonld against the SHACL shape.
// Implementations are shared across all harvest goroutines and must be safe for concurrent use
type ShaclValidator interface {
	// Validate reports whether the jsonld conforms and the validation report message;
	// an error means validation could not be performed
	Validate(ctx context.Context, jsonld string) (conforms bool, message string, err error)
	// Close releases any resources held by the validator
	Close() error
}

// validate jsonld with the validator and convert the result into the error types the harvest expects
func validate_shacl(ctx context.Context, validator ShaclValidator, urlSource string, jsonldContent string) error {
	// no point in validating if there is no jsonld content; we don't want to be saving empty files
	if jsonldContent == "" {
		return pkg.ShaclInfo{ShaclStatus: pkg.ShaclSkipped, ShaclValidationMessage: "no jsonld to validate"}
	}
	ctx, subspan := opentelemetry.SubSpanFromCtxWithName(ctx, "shacl_validation")
	defer subspan.End()
	log.Tracef("validating jsonld of byte size %d", len(jsonldContent))
	conforms, message, err := validator.Validate(ctx, jsonldContent)
	if err != nil {
		return err
	} else if !conforms {
		subspan.SetStatus(codes.Error, message)
		return ShaclValidationFailureError{ShaclErrorMessage: message, Url: urlSource}
	}
	return nil
}

// GrpcShaclValidator validates jsonld by sending it to a remote SHACL validation gRPC server
type GrpcShaclValidator struct {
	clients []protoBuild.ShaclValidatorClient
	conns   []*grpc.ClientConn
	next    atomic.Uint64
}

// NewGrpcShaclValidator opens several connections to the gRPC server at the address;
// requests are spread across them so a multi-process validator can use all of its processes.
// Connections are established lazily on first use
func NewGrpcShaclValidator(shaclAddress string) (*GrpcShaclValidator, error) {
	if shaclAddress == "" {
		return nil, errors.New("shacl grpc address cannot be empty")
	}
	validator := &GrpcShaclValidator{}
	for range shaclGrpcConnections {
		conn, err := newShaclGrpcConn(shaclAddress)
		if err != nil {
			_ = validator.Close()
			return nil, err
		}
		validator.conns = append(validator.conns, conn)
		validator.clients = append(validator.clients, protoBuild.NewShaclValidatorClient(conn))
	}
	return validator, nil
}

// NewGrpcShaclValidatorFromClients creates a validator from existing gRPC clients
func NewGrpcShaclValidatorFromClients(clients ...protoBuild.ShaclValidatorClient) *GrpcShaclValidator {
	return &GrpcShaclValidator{clients: clients}
}

func (v *GrpcShaclValidator) Validate(ctx context.Context, jsonld string) (bool, string, error) {
	client := v.clients[(v.next.Add(1)-1)%uint64(len(v.clients))]
	reply, err := client.Validate(ctx, &protoBuild.JsoldValidationRequest{Jsonld: jsonld})
	if err != nil {
		return false, "", fmt.Errorf("failed sending validation request to gRPC server: %w", err)
	}
	return reply.Valid, reply.Message, nil
}

func (v *GrpcShaclValidator) Close() error {
	var errs []error
	for _, conn := range v.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

// Create a separate gRPC connection to the SHACL validator.
// Each connection is its own HTTP/2 connection, so opening several lets a
// multi-process validator spread requests across its processes
func newShaclGrpcConn(shaclAddress string) (*grpc.ClientConn, error) {
	// 32 megabytes is the current upperbound of the jsonld documents we will validate
	// beyond that is a sign that the document may be too large or incorrectly formatted
	thirtyTwoMB := 32 * 1024 * 1024
	conn, err := grpc.NewClient(shaclAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithMaxHeaderListSize(uint32(thirtyTwoMB)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}
	return conn, nil
}

// LocalShaclValidator validates jsonld in process against the bundled Geoconnex SHACL shape
type LocalShaclValidator struct {
	validator shacl_validator.ShaclValidator
}

// NewLocalShaclValidator creates an in process validator. Remote JSON-LD contexts
// are fetched with the crawler http client and cached for the lifetime of the
// validator; the schema.org context is bundled so it is never fetched
func NewLocalShaclValidator() (*LocalShaclValidator, error) {
	validator, err := shacl_validator.NewGeoconnexShaclValidator()
	if err != nil {
		return nil, err
	}
	loader, err := shacl_validator.NewDefaultCachingDocumentLoader(common.NewCrawlerClient())
	if err != nil {
		return nil, err
	}
	validator.SetDocumentLoader(loader)
	return &LocalShaclValidator{validator: validator}, nil
}

func (v *LocalShaclValidator) Validate(ctx context.Context, jsonld string) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	report, err := v.validator.ValidateJsonldString(jsonld)
	if err != nil {
		return false, "", fmt.Errorf("failed to parse jsonld for shacl validation: %w", err)
	}
	return report.Conforms, shacl_validator.ReportText(report), nil
}

func (v *LocalShaclValidator) Close() error {
	return nil
}

// NewShaclValidatorFromConfig returns the validator to use for a harvest:
// the local validator if requested, otherwise the gRPC validator if an address is set.
// Returns nil if neither is configured, which means validation is skipped
func NewShaclValidatorFromConfig(useLocal bool, grpcAddress string) (ShaclValidator, error) {
	switch {
	case useLocal && grpcAddress != "":
		return nil, errors.New("only one of local shacl validation or a shacl grpc endpoint can be used")
	case useLocal:
		log.Info("Validating jsonld against SHACL shapes locally")
		validator, err := NewLocalShaclValidator()
		if err != nil {
			return nil, err
		}
		return validator, nil
	case grpcAddress != "":
		log.Infof("Validating jsonld against SHACL shapes with the gRPC server at %s", grpcAddress)
		validator, err := NewGrpcShaclValidator(grpcAddress)
		if err != nil {
			return nil, err
		}
		return validator, nil
	default:
		log.Warn("No shacl validation configured. Skipping shacl validation...")
		return nil, nil
	}
}
