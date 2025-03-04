package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// test reading in a sample config
func TestReadConfig(t *testing.T) {

	configPath := "../../config/iow"

	conf, err := NewNabuConfig(configPath, "nabuconfig.yaml")
	require.NoError(t, err)
	require.Equal(t, conf.Minio.Accesskey, "minioadmin")
	require.Equal(t, conf.Minio.Secretkey, "minioadmin")
	require.Equal(t, conf.Context.Cache, true)

}
