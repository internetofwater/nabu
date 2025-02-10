package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// test reading in a sample config
func TestReadConfig(t *testing.T) {

	configPath := "../../config/iow"

	conf, err := ReadNabuConfig(configPath, "nabuconfig.yaml")
	require.NoError(t, err)
	require.Equal(t, conf.Minio.Accesskey, "amazingaccesskey")
	require.Equal(t, conf.Minio.Secretkey, "amazingsecretkey")
	require.Equal(t, conf.Context.Cache, true)

}
