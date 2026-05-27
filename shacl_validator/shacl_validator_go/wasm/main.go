// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log"

	shacl_validator "github.com/internetofwater/nabu/shacl_validator/shacl_validator_go"
)

func main() {
	validator, err := shacl_validator.NewGeoconnexShaclValidator()
	if err != nil {
		log.Fatalf("failed to create SHACL validator: %v", err)
	}
	report, err := validator.ValidateArbitraryJsonld("{}")
	if err != nil {
		log.Fatalf("SHACL validation error: %v", err)
	}
	if report.Conforms {
		log.Println("Data conforms to SHACL shape")
	} else {
		log.Println("Data does not conform to SHACL shape")
		for _, result := range report.Results {
			log.Printf("%v", result)
		}
	}

}
