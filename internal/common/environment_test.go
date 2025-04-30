// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPROFILING_ENABLED(t *testing.T) {
	profile_env := os.Setenv("NABU_PROFILING", "True")
	require.NoError(t, profile_env)
	require.Equal(t, true, PROFILING_ENABLED())

	profile_env = os.Setenv("NABU_PROFILING", "False")
	require.NoError(t, profile_env)
	require.Equal(t, false, PROFILING_ENABLED())
}
