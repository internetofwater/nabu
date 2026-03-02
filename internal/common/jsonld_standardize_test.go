// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStandardizeJsonldContextWithMutation(t *testing.T) {

	gages_jsonld_file, err := os.ReadFile("./testdata/standardize_jsonld/gage_jsonld.jsonld")
	require.NoError(t, err)
	var gages_jsonld map[string]any
	err = json.Unmarshal(gages_jsonld_file, &gages_jsonld)
	require.NoError(t, err)

	gages_jsonld_expected_file, err := os.ReadFile("./testdata/standardize_jsonld/gage_jsonld_standardized.jsonld")
	require.NoError(t, err)
	var gages_jsonld_expected map[string]any
	err = json.Unmarshal(gages_jsonld_expected_file, &gages_jsonld_expected)
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
			name:    "no @context",
			input:   map[string]any{"foo": "bar"},
			want:    map[string]any{"foo": "bar"},
			wantErr: true,
		},
		{
			name:    "@context with http and www",
			input:   map[string]any{"@context": "http://www.example.com/"},
			want:    map[string]any{"@context": "https://www.example.com/"},
			wantErr: false,
		},
		{
			name:    "@context with https and no www",
			input:   map[string]any{"@context": "https://example.com/"},
			want:    map[string]any{"@context": "https://example.com/"},
			wantErr: false,
		},
		{
			name:    "@context as an array",
			input:   map[string]any{"@context": []any{"http://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			want:    map[string]any{"@context": []any{"https://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			wantErr: false,
		},
		{
			name:    "strip #",
			input:   map[string]any{"@context": []any{"http://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			want:    map[string]any{"@context": []any{"https://www.example.com/", "https://example.com/"}, "@type": "Organization", "dummy_key": "dummy_value"},
			wantErr: false,
		},
		{
			name:    "full gages jsonld test",
			input:   gages_jsonld,
			want:    gages_jsonld_expected,
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
