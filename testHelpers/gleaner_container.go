// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package testhelpers

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GleanerContainer struct {
	// the handle to the container
	Container testcontainers.Container
	// the exit code of the gleaner executable
	ExitCode int
	// the string output from the container
	Logs string
	// the name of the container; useful for debugging and looking into logs on docker desktop
	Name string
}

// Create a handle to a struct which allows for easy handling of the minio container
func NewGleanerContainer(configPath string, cmd []string, networkName string) (GleanerContainer, error) {
	ctx := context.Background()

	fullCmd := append([]string{"--cfg", "/app/gleanerconfig.yaml"}, cmd...)

	gleanerTestImage := os.Getenv("GLEANER_TEST_IMAGE")

	if gleanerTestImage == "" {
		gleanerTestImage = "internetofwater/gleaner:latest"
	}

	req := testcontainers.ContainerRequest{
		Image: gleanerTestImage,
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      configPath,
				ContainerFilePath: "/app/gleanerconfig.yaml",
			},
		},
		// wait for the crawl to finish so our tests operate on the full data
		WaitingFor: wait.ForExit(),
		Cmd:        fullCmd,
		// Entrypoint: []string{"/bin/sh", "-c", "while true; do sleep 30; done"}, <-- used for debugging if we need to go inside to inspect the network
		Networks: []string{networkName},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("failed launching gleaner container: %w", err)
	}

	logs, err := genericContainer.Logs(ctx)
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("failed getting logs from gleaner container: %w", err)
	}
	logBytes, err := io.ReadAll(logs)
	defer logs.Close()
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("failed reading logs from gleaner container: %w", err)
	}

	state, err := genericContainer.State(ctx)
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("failed getting state from gleaner container: %w", err)
	}

	inspectResult, err := genericContainer.Inspect(ctx)
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("failed getting name from gleaner container: %w", err)
	}

	return GleanerContainer{Container: genericContainer, ExitCode: state.ExitCode, Logs: string(logBytes), Name: inspectResult.Name}, nil

}
