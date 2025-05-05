// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProvData_ToJsonLD(t *testing.T) {
	prov := ProvData{
		RESID:  "res123",
		SHA:    "abc123",
		PID:    "org456",
		SOURCE: "source",
		DATE:   "2025-01-01T00:00:00Z",
		RUNID:  "run789",
		URN:    "urn:example:789",
		PNAME:  "Test Org",
		DOMAIN: "https://example.org",
	}

	reader := prov.toJsonLD()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)

	jsonStr := string(content)

	// Check some known substrings
	expectedFields := []string{
		`"@id": "org456"`,
		`"rdf:name": "Test Org"`,
		`"@id": "res123"`,
		`"@value": "2025-01-01T00:00:00Z"`,
		`"@id": "https://gleaner.io/id/collection/abc123"`,
	}

	for _, field := range expectedFields {
		if !strings.Contains(jsonStr, field) {
			t.Errorf("Expected field %q not found in output", field)
		}
	}

	// Try to decode to generic JSON to check validity
	var js map[string]interface{}
	if err := json.Unmarshal(content, &js); err != nil {
		t.Errorf("Output is not valid JSON: %v", err)
	}
}
