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

// Returned when the jsonld has neither a schema:Place nor schema:Dataset node
const MissingPlaceOrDatasetTypeMessage = "SHACL Validation failed: the top level node of the jsonld must have '@type': 'schema:Place' or '@type': 'schema:Dataset'"

type ShaclValidator struct {
	shacl_shape *shacl.Graph
	// the turtle source of the shape, kept so it can be served back to clients
	shacl_shape_ttl string
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
	return NewShaclValidatorFromTurtle(shapes.GeoconnexTTL)
}

// Create a validator from a SHACL shape in turtle format
func NewShaclValidatorFromTurtle(ttl string) (ShaclValidator, error) {
	shape, err := shacl.LoadTurtleString(ttl, "")
	if err != nil {
		return ShaclValidator{}, err
	}

	return ShaclValidator{shacl_shape: shape, shacl_shape_ttl: ttl}, nil
}

// Create a validator from a SHACL shape turtle file on disk
func NewShaclValidatorFromFile(path string) (ShaclValidator, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ShaclValidator{}, err
	}
	return NewShaclValidatorFromTurtle(string(data))
}

// The SHACL shape used for validation in turtle format
func (v *ShaclValidator) ShapeTurtle() string {
	return v.shacl_shape_ttl
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
		error_msg := shacl.Literal(MissingPlaceOrDatasetTypeMessage, "", "")
		validation_result := shacl.ValidationResult{ResultMessages: []shacl.Term{error_msg}, ResultSeverity: shacl.SHViolation}
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

// Render a validation report as text in the same format as pyshacl
// so that clients of the python validator see the same messages
func ReportText(report shacl.ValidationReport) string {
	var sb strings.Builder
	sb.WriteString("Validation Report\n")
	fmt.Fprintf(&sb, "Conforms: %s\n", pythonBool(report.Conforms))
	if len(report.Results) > 0 {
		fmt.Fprintf(&sb, "Results (%d):\n", len(report.Results))
		for _, result := range report.Results {
			writeResultText(&sb, result, 0)
		}
	}
	return sb.String()
}

func pythonBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// the local name of an IRI, i.e. the part after the last '#' or '/'
func localName(iri string) string {
	if i := strings.LastIndexAny(iri, "#/"); i >= 0 {
		return iri[i+1:]
	}
	return iri
}

func writeResultText(sb *strings.Builder, vr shacl.ValidationResult, depth int) {
	indent := strings.Repeat("\t", depth)

	var severityTitle string
	switch {
	case vr.ResultSeverity.Equal(shacl.SHViolation):
		severityTitle = "Constraint Violation"
	case vr.ResultSeverity.Equal(shacl.SHWarning):
		severityTitle = "Validation Warning"
	case vr.ResultSeverity.Equal(shacl.SHInfo):
		severityTitle = "Validation Info"
	default:
		severityTitle = "Validation Result"
	}

	if vr.SourceConstraintComponent.IsNone() {
		fmt.Fprintf(sb, "%s%s:\n", indent, severityTitle)
	} else {
		component := vr.SourceConstraintComponent.Value()
		fmt.Fprintf(sb, "%s%s in %s (%s):\n", indent, severityTitle, localName(component), component)
	}

	field := func(name string, term shacl.Term) {
		if !term.IsNone() {
			fmt.Fprintf(sb, "%s\t%s: %s\n", indent, name, term)
		}
	}
	if !vr.ResultSeverity.IsNone() {
		fmt.Fprintf(sb, "%s\tSeverity: sh:%s\n", indent, localName(vr.ResultSeverity.Value()))
	}
	field("Source Shape", vr.SourceShape)
	field("Focus Node", vr.FocusNode)
	field("Value Node", vr.Value)
	field("Result Path", vr.ResultPath)
	for _, msg := range vr.ResultMessages {
		fmt.Fprintf(sb, "%s\tMessage: %s\n", indent, msg.Value())
	}
	if len(vr.Details) > 0 {
		fmt.Fprintf(sb, "%s\tDetails:\n", indent)
		for _, detail := range vr.Details {
			writeResultText(sb, detail, depth+2)
		}
	}
}
