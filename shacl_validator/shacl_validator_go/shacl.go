// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/internetofwater/nabu/shacl_validator/shapes"
	"github.com/tggo/goRDFlib/jsonld"
	"github.com/tggo/goRDFlib/shacl"
)

type ShaclValidator struct {
	shacl_shape *shacl.Graph
}

func (v *ShaclValidator) ValidateArbitraryJsonld(input string) (shacl.ValidationReport, error) {
	if input == "" {
		return shacl.ValidationReport{}, fmt.Errorf("shacl validation input cannot be empty")
	}
	// URL input
	if strings.HasPrefix(input, "http://") || strings.HasPrefix(input, "https://") {
		resp, err := http.Get(input)
		if err != nil {
			return shacl.ValidationReport{}, err
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return shacl.ValidationReport{}, fmt.Errorf("failed to fetch URL: %s (status %d)", input, resp.StatusCode)
		}

		jsonldGraph, err := shacl.LoadJsonLD(resp.Body, "", jsonld.WithUnboundedLines())
		if err != nil {
			return shacl.ValidationReport{}, err
		}

		return v.Validate(jsonldGraph)
	}

	// File path input
	if stat, err := os.Stat(input); err == nil && !stat.IsDir() {
		data, err := os.ReadFile(input)
		if err != nil {
			return shacl.ValidationReport{}, err
		}

		return v.ValidateJsonldString(string(data))
	}

	// Otherwise assume raw JSON-LD string
	return v.ValidateJsonldString(input)
}

func NewGeoconnexShaclValidator() (ShaclValidator, error) {
	shape, err := shacl.LoadTurtleString(shapes.GeoconnexTTL, "")
	if err != nil {
		return ShaclValidator{}, err
	}

	return ShaclValidator{shacl_shape: shape}, nil
}

func (v *ShaclValidator) ValidateJsonldString(data string) (shacl.ValidationReport, error) {
	jsonld_shape, err := shacl.LoadJsonLDString(data, "", jsonld.WithUnboundedLines())
	if err != nil {
		return shacl.ValidationReport{}, err
	}
	return v.Validate(jsonld_shape)
}

func (v *ShaclValidator) Validate(data *shacl.Graph) (shacl.ValidationReport, error) {
	rdfs_type_term := shacl.IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

	place_subjects := data.Subjects(rdfs_type_term, shacl.IRI("https://schema.org/Place"))
	dataset_subjects := data.Subjects(rdfs_type_term, shacl.IRI("https://schema.org/Dataset"))

	if len(place_subjects) == 0 && len(dataset_subjects) == 0 {
		error_msg := shacl.Literal("Data must be of @type schema:Place or schema:Dataset", "", "")
		validation_result := shacl.ValidationResult{Value: error_msg, ResultSeverity: shacl.SHViolation}
		return shacl.ValidationReport{
			Conforms: false,
			Results:  []shacl.ValidationResult{validation_result},
		}, nil
	}

	return shacl.Validate(data, v.shacl_shape), nil
}

func PrintValidationResult(vr shacl.ValidationResult) string {
	var severityPrefix string

	switch vr.ResultSeverity {
	case shacl.SHViolation:
		severityPrefix = "SHACL Violation"
	case shacl.SHWarning:
		severityPrefix = "SHACL Warning"
	case shacl.SHInfo:
		severityPrefix = "SHACL Info"
	default:
		severityPrefix = "SHACL Unknown"
	}

	var msgs []string
	for _, m := range vr.ResultMessages {
		msgs = append(msgs, fmt.Sprint(m))
	}

	parts := []string{
		fmt.Sprintf(
			"%s: Node=%s Path=%s Value=%s Shape=%s Constraint=%s Component=%s",
			severityPrefix,
			vr.FocusNode,
			vr.ResultPath,
			vr.Value,
			vr.SourceShape,
			vr.SourceConstraint,
			vr.SourceConstraintComponent,
		),
	}

	// messages (optional)
	if len(msgs) > 0 {
		parts = append(parts, fmt.Sprintf("[%s]", strings.Join(msgs, "; ")))
	}

	// details (optional)
	if len(vr.Details) > 0 {
		var details []string
		for _, d := range vr.Details {
			details = append(details, PrintValidationResult(d))
		}
		parts = append(parts, fmt.Sprintf("[%s]", strings.Join(details, " | ")))
	}

	return "{" + strings.Join(parts, " ") + "}"
}
