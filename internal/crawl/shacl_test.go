// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package crawl

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/internetofwater/nabu/internal/common/projectpath"
	"github.com/internetofwater/nabu/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// a mock SHACL validator for testing
type mockShaclValidator struct{}

func (m *mockShaclValidator) Validate(ctx context.Context, jsonld string) (bool, string, error) {
	// for testing purposes, any jsonld missing the @context block is invalid
	// in the future you could extend this to do more complex validation
	if !strings.Contains(jsonld, "@context") {
		return false, "invalid jsonld content", nil
	}
	return true, "valid", nil
}

func (m *mockShaclValidator) Close() error {
	return nil
}

var _ ShaclValidator = &mockShaclValidator{}

// a mock SHACL validator that always fails to perform validation
type erroringShaclValidator struct{}

func (m *erroringShaclValidator) Validate(ctx context.Context, jsonld string) (bool, string, error) {
	return false, "", errors.New("failed to communicate with the shacl validator")
}

func (m *erroringShaclValidator) Close() error {
	return nil
}

func TestNewShaclValidatorFromConfig(t *testing.T) {
	validator, err := NewShaclValidatorFromConfig(false)
	require.NoError(t, err)
	require.Nil(t, validator, "no validator should be created if validation is not configured")

	validator, err = NewShaclValidatorFromConfig(true)
	require.NoError(t, err)
	require.IsType(t, &LocalShaclValidator{}, validator)
}

func readShaclTestdata(t *testing.T, path ...string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(append([]string{projectpath.Root, "shacl_validator", "testdata"}, path...)...))
	require.NoError(t, err)
	return string(data)
}

func TestLocalShaclValidator(t *testing.T) {
	validator, err := NewLocalShaclValidator()
	require.NoError(t, err)

	const url = "https://example.com/feature"

	require.NoError(t, validate_shacl(context.Background(), validator, url, readShaclTestdata(t, "valid", "rise.jsonld")))

	err = validate_shacl(context.Background(), validator, url, readShaclTestdata(t, "invalid", "no_geo.jsonld"))
	var shaclErr ShaclValidationFailureError
	require.ErrorAs(t, err, &shaclErr)
	require.Equal(t, url, shaclErr.Url)
	require.Contains(t, shaclErr.ShaclErrorMessage, "Places must include geometry in WKT format")

	// malformed jsonld is an error with validation itself rather than a shacl failure
	err = validate_shacl(context.Background(), validator, url, "{not json")
	require.Error(t, err)
	require.NotErrorAs(t, err, &shaclErr)

	err = validate_shacl(context.Background(), validator, url, "")
	var info pkg.ShaclInfo
	require.ErrorAs(t, err, &info)
	require.Equal(t, pkg.ShaclSkipped, info.ShaclStatus)
}

func TestLocalShaclValidatorConcurrent(t *testing.T) {
	validator, err := NewLocalShaclValidator()
	require.NoError(t, err)
	valid := readShaclTestdata(t, "valid", "rise.jsonld")
	invalid := readShaclTestdata(t, "invalid", "no_geo.jsonld")

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			data, expected := valid, true
			if i%2 == 0 {
				data, expected = invalid, false
			}
			conforms, _, err := validator.Validate(context.Background(), data)
			assert.NoError(t, err)
			assert.Equal(t, expected, conforms)
		})
	}
	wg.Wait()
}
