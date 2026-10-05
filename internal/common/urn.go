// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"strings"
)

/*
This file represents all operations for defining a URN

A URN can be used to identify either a graph or a
prefix path in S3. Thus you can convert between
the two to perform synch operations
*/

const baseURN = "urn:iow"

type URN = string

// Map a s3 prefix to a URN
// This is essentially just a serialized path that can be used for identifying a graph
// We use a simple
func MakeURN(s3Prefix string) (URN, error) {
	if s3Prefix == "" || s3Prefix == "." {
		return "", fmt.Errorf("prefix cannot be empty")
	} else if !strings.Contains(s3Prefix, "/") {
		return "", fmt.Errorf("prefix must contain at least one '/'")
	} else if strings.Contains(s3Prefix, "//") {
		return "", fmt.Errorf("prefix cannot contain double slashes")
	}

	resultURN := baseURN
	splitOnSlash := strings.Split(s3Prefix, "/")
	for _, part := range splitOnSlash {
		if part == "" {
			break
		}
		resultURN += ":" + part
	}
	return resultURN, nil
}
