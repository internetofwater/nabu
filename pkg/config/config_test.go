package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// test reading in a sample config

func TestReadConfig(t *testing.T) {

	configPath := "../../config/iow"

	_, err := ReadNabuConfig("nabuconfig.yaml", configPath)
	require.NoError(t, err)

}
