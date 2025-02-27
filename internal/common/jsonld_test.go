package common

import (
	"nabu/internal/common/projectpath"
	"nabu/pkg/config"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/require"
)

// generate a caching jsonld processor for testing
// uses the iow config to specify the context maps
func newCachingJsonldProcessor(t *testing.T) (*ld.JsonLdProcessor, *ld.JsonLdOptions, error) {
	configPath := filepath.Join(projectpath.Root, "config/iow")
	absPath, err := filepath.Abs(configPath)
	require.NoError(t, err)
	conf, err := config.ReadNabuConfig(absPath, "nabuconfig.yaml")
	require.NoError(t, err)
	return NewJsonldProcessor(conf)
}

func TestCreateNewProcessor(t *testing.T) {

	t.Run("empty config returns blank processor", func(t *testing.T) {
		_, _, err := NewJsonldProcessor(config.NabuConfig{})
		require.NoError(t, err)
	})

	t.Run("use full config with caching", func(t *testing.T) {
		processor, options, err := newCachingJsonldProcessor(t)
		require.NoError(t, err)
		loader := options.DocumentLoader
		require.IsType(t, &ld.CachingDocumentLoader{}, loader)
		require.NotNil(t, processor)

		const simpleJSONLDExample = `{
			"@context": "https://json-ld.org/contexts/person.jsonld",
			"@id": "http://dbpedia.org/resource/John_Lennon",
			"name": "John Lennon",
			"born": "1940-10-09",
			"spouse": "http://dbpedia.org/resource/Cynthia_Lennon"
			}`
		nq, err := JsonldToNQ(simpleJSONLDExample, processor, options)
		require.NoError(t, err)

		birthDateLine := `<http://dbpedia.org/resource/John_Lennon> <http://schema.org/birthDate> "1940-10-09"`
		require.Contains(t, nq, birthDateLine)

	})
}

func TestConcurrentCachingInJsonldProc(t *testing.T) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	workers := 100
	wg.Add(workers)

	bytes, err := os.ReadFile("testdata/hu02JsonldCtx.jsonld")
	require.NoError(t, err)

	processor, options, err := newCachingJsonldProcessor(t)
	if err != nil {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
		return
	}

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			require.IsType(t, &ld.CachingDocumentLoader{}, options.DocumentLoader)

			nq, err := JsonldToNQ(string(bytes), processor, options)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}

			if !contains(nq, "hu02") {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Check errors after all goroutines finish
	if len(errs) > 0 {
		for _, err := range errs {
			t.Error(err)
		}
		t.FailNow()
	}
}

// Helper function to check if a string contains a substring (to replace `require.Contains`)
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
