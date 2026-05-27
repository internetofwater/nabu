// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package shacl_validator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tggo/goRDFlib/shacl"
)

func loadValidator(t *testing.T) *ShaclValidator {
	t.Helper()

	shapePath := filepath.Join("..", "shapes", "geoconnex.ttl")

	shapeGraph, err := shacl.LoadTurtleFile(shapePath)
	require.NoError(t, err)

	return &ShaclValidator{
		shacl_shape: shapeGraph,
	}
}

func TestAllValidCases(t *testing.T) {
	validator := loadValidator(t)

	validDir := filepath.Join("..", "testdata", "valid")

	files, err := os.ReadDir(validDir)
	require.NoError(t, err)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		t.Run(file.Name(), func(t *testing.T) {
			path := filepath.Join(validDir, file.Name())

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			report, err := validator.ValidateJsonldString(string(data))
			require.NoError(t, err)

			if !report.Conforms {
				assert.True(
					t,
					report.Conforms,
					"SHACL validation failed for %s:\n%s",
					file.Name(),
					report.Results,
				)
			}

		})
	}
}

func TestAllInvalidCases(t *testing.T) {
	validator := loadValidator(t)

	invalidDir := filepath.Join("..", "testdata", "invalid")

	files, err := os.ReadDir(invalidDir)
	require.NoError(t, err)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		t.Run(file.Name(), func(t *testing.T) {
			path := filepath.Join(invalidDir, file.Name())

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			report, err := validator.ValidateJsonldString(string(data))
			require.NoError(t, err)

			assert.False(
				t,
				report.Conforms,
				"SHACL validation unexpectedly passed for %s:\n%s",
				file.Name(),
				report.Results,
			)
		})
	}
}

// func TestRemoteData(t *testing.T) {
// 	const url = "https://reference.geoconnex.us/collections/hu02/items/12?f=jsonld"

// 	validator := loadValidator(t)

// 	report, err := validator.ValidateArbitraryJsonld(url)
// 	require.Error(t, err)
// 	assert.False(t, report.Conforms)
// }
