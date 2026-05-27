// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	shacl_validator "github.com/internetofwater/nabu/shacl_validator/shacl_validator_go"
	"github.com/tggo/goRDFlib/shacl"
)

func ShaclValidate(input string) (shacl.ValidationReport, error) {
	validator, err := shacl_validator.NewGeoconnexShaclValidator()
	if err != nil {
		return shacl.ValidationReport{}, err
	}
	return validator.ValidateArbitraryJsonld(input)
}
