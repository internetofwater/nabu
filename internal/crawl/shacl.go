// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"fmt"

	"github.com/internetofwater/nabu/internal/common"
	"github.com/internetofwater/nabu/internal/opentelemetry"
	"github.com/internetofwater/nabu/pkg"
	shacl_validator "github.com/internetofwater/nabu/shacl_validator/shacl_validator_go"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
)

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

// NewShaclValidatorFromConfig returns the validator to use for a harvest.
// Returns nil if validation is not enabled, which means validation is skipped
func NewShaclValidatorFromConfig(useLocal bool) (ShaclValidator, error) {
	if !useLocal {
		log.Warn("No shacl validation configured. Skipping shacl validation...")
		return nil, nil
	}
	log.Info("Validating jsonld against SHACL shapes locally")
	validator, err := NewLocalShaclValidator()
	if err != nil {
		return nil, err
	}
	return validator, nil
}
