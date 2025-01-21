package config

import "testing"

// test reading in a sample config

func TestReadConfig(t *testing.T) {

	configPath := "../config/iow/nabuconfig.yaml"

	ReadNabuConfig(configPath, configPath)
}
