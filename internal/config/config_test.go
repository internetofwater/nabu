// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// test reading in a sample config
func TestReadConfig(t *testing.T) {

	conf, err := ReadNabuConfig("testdata", "nabuconfig.yaml")
	require.NoError(t, err)
	require.Equal(t, conf.Minio.Accesskey, "minioadmin")
	require.Equal(t, conf.Minio.Secretkey, "minioadmin")
	require.Equal(t, conf.Context.Cache, true)

}
