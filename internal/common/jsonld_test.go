package common

import (
	"nabu/internal/common/projectpath"
	"nabu/pkg/config"
	"path/filepath"
	"testing"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"
)

func TestCreateNewProcessor(t *testing.T) {

	t.Run("empty config returns blank processor", func(t *testing.T) {
		_, _, err := NewJsonldProcessor(config.NabuConfig{})
		require.NoError(t, err)
	})

	t.Run("use full config with caching", func(t *testing.T) {
		configPath := filepath.Join(projectpath.Root, "config/iow")
		absPath, err := filepath.Abs(configPath)
		require.NoError(t, err)
		conf, err := config.ReadNabuConfig(absPath, "nabuconfig.yaml")
		require.NoError(t, err)
		processor, options, err := NewJsonldProcessor(conf)
		require.NoError(t, err)
		loader := options.DocumentLoader
		require.IsType(t, &ld.CachingDocumentLoader{}, loader)
		require.NotNil(t, processor)
	})
}
