package testhelpers

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGleanerContainerVersion(t *testing.T) {
	container, err := StartGleanerContainer("--help")
	require.NoError(t, err, "Failed to start Gleaner container")
	// defer container.Container.Terminate(container.Context)

	// Fetch the logs from the container to verify the output
	logs, err := container.Container.Logs(context.Background())
	require.NoError(t, err, "Failed to get logs from Gleaner container")

	// Read the logs and verify the expected output
	buf := new(strings.Builder)
	_, err = io.Copy(buf, logs)
	assert.NoError(t, err, "Failed to read logs from Gleaner container")

	output := buf.String()
	t.Logf("Gleaner version output: %s", output)

	assert.Contains(t, output, "version", "Expected version information not found in logs. Gleaner likely did not launch correctly")
}
