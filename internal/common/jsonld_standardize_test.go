// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func fileToMap(filePath string) (map[string]any, error) {
	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	err = json.Unmarshal(fileBytes, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func TestStandardizeJsonldContextWithMutation(t *testing.T) {

	gages_jsonld_incorrect, err := fileToMap("./testdata/standardize_jsonld/gage_jsonld_incorrect.jsonld")
	require.NoError(t, err)

	gages_jsonld_expected, err := fileToMap("./testdata/standardize_jsonld/gage_jsonld_standardized.jsonld")
	require.NoError(t, err)

	gages_jsonld_file_with_vocab, err := fileToMap("./testdata/standardize_jsonld/gage_jsonld_incorrect_using_vocab.jsonld")
	require.NoError(t, err)
	gages_jsonld_expected_with_vocab, err := fileToMap("./testdata/standardize_jsonld/gage_jsonld_standardized_using_vocab.jsonld")
	require.NoError(t, err)

	tests := []struct {
		name    string
		input   map[string]any
		want    map[string]any
		wantErr bool
	}{
		{
			name:    "nil input",
			input:   nil,
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no @context returns an error",
			input:   map[string]any{"foo": "bar"},
			want:    map[string]any{"foo": "bar"},
			wantErr: true,
		},
		{
			name:    "@context with http and www stays the same",
			input:   map[string]any{"@context": "http://www.example.com/"},
			want:    map[string]any{"@context": "http://www.example.com/"},
			wantErr: false,
		},
		{
			name:    "@context with http schema is changed to https",
			input:   map[string]any{"@context": "http://schema.org/"},
			want:    map[string]any{"@context": "https://schema.org/"},
			wantErr: false,
		},
		{
			name:    "@context as an array without relevant changes stays the same",
			input:   map[string]any{"@context": []any{"http://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			want:    map[string]any{"@context": []any{"http://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			wantErr: false,
		},
		{
			name:    "# is not stripped from @context IRIs",
			input:   map[string]any{"@context": []any{"http://www.example.com/#", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			want:    map[string]any{"@context": []any{"http://www.example.com/#", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			wantErr: false,
		},
		{
			name:    "@context as an array with @vocab and http schema is changed to https and @vocab is also modified",
			input:   map[string]any{"@context": map[string]any{"@vocab": "http://schema.org/", "foo": "http://www.example.com/", "schema": "http://schema.org/", "hyf": "https://www.opengis.net/def/appschema/hy_features/hyf"}},
			want:    map[string]any{"@context": map[string]any{"@vocab": "https://schema.org/", "foo": "http://www.example.com/", "schema": "https://schema.org/", "hyf": "https://www.opengis.net/def/schema/hy_features/hyf/"}},
			wantErr: false,
		},
		{
			name:    "full gages jsonld test",
			input:   gages_jsonld_incorrect,
			want:    gages_jsonld_expected,
			wantErr: false,
		},
		{
			name:    "full gages jsonld test with @vocab to ensure special iri keys are still processed",
			input:   gages_jsonld_file_with_vocab,
			want:    gages_jsonld_expected_with_vocab,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StandardizeJsonldContextWithMutation(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("StandardizeJsonldContextWithMutation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			got_as_bytes, err := json.Marshal(got)
			require.NoError(t, err)
			want_as_bytes, err := json.Marshal(tt.want)
			require.NoError(t, err)

			require.JSONEq(t, string(got_as_bytes), string(want_as_bytes))
		})
	}
}

func TestStandardizeIri(t *testing.T) {

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "http schema is changed to https",
			input: "http://schema.org/",
			want:  "https://schema.org/",
		},
		{
			name:  "http://www.opengeospatial.org/standards/waterml2/hy_features",
			input: "http://www.opengeospatial.org/standards/waterml2/hy_features",
			want:  "https://www.opengis.net/def/schema/hy_features/hyf/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := standardizeIRI(tt.input)
			require.Equal(t, tt.want, got)
		})
	}

}
