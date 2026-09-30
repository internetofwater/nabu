// Copyright 2026 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	shacl_validator "github.com/internetofwater/nabu/shacl_validator/shacl_validator_go"
	"github.com/internetofwater/nabu/shacl_validator/shapes"
	log "github.com/sirupsen/logrus"
	"github.com/tggo/goRDFlib/shacl"
)

func ShaclValidate(input string) (shacl.ValidationReport, error) {
	validator, err := shacl_validator.NewGeoconnexShaclValidator()
	if err != nil {
		return shacl.ValidationReport{}, err
	}
	return validator.ValidateArbitraryJsonld(input)
}

// Run the shacl subcommand
func Shacl(ctx context.Context, cmd ShaclCmd) error {
	switch {
	case cmd.PrintShape:
		fmt.Println(shapes.GeoconnexTTL)
		return nil
	case cmd.Serve != nil:
		return ShaclServe(ctx, *cmd.Serve)
	case cmd.Validate != nil:
		return shaclValidateCmd(*cmd.Validate)
	default:
		return fmt.Errorf("a shacl subcommand of either 'validate' or 'serve' is required")
	}
}

func shaclValidateCmd(cmd ShaclValidateCmd) error {
	if cmd.Input == "-" {
		inputBytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("error reading from stdin: %w", err)
		}
		cmd.Input = string(inputBytes)
	}

	report, err := ShaclValidate(cmd.Input)
	if err != nil {
		return err
	}
	if report.Conforms {
		log.Info("Data conforms to SHACL shape")
		return nil
	}
	log.Error("Data does not conform to SHACL shape")
	for _, result := range report.Results {
		log.Error(shacl_validator.PrintValidationResult(result))
	}
	return fmt.Errorf("found %d shacl validation errors", len(report.Results))
}

// Serve the SHACL validation http endpoints until interrupted
func ShaclServe(ctx context.Context, cmd ShaclServeCmd) error {
	var validator shacl_validator.ShaclValidator
	var err error
	if cmd.ShaclFile != "" {
		validator, err = shacl_validator.NewShaclValidatorFromFile(cmd.ShaclFile)
	} else {
		validator, err = shacl_validator.NewGeoconnexShaclValidator()
	}
	if err != nil {
		return fmt.Errorf("failed to load SHACL shape: %w", err)
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return shacl_validator.Serve(ctx, &validator, fmt.Sprintf("0.0.0.0:%d", cmd.HttpPort))
}
