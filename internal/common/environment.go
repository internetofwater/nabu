// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"os"
	"strings"
)

func PROFILING_ENABLED() bool {
	profile_env := os.Getenv("NABU_PROFILING")
	profiling_enabled := strings.ToLower(profile_env) == "true"
	return profiling_enabled
}
