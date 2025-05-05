// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package synchronizer_test

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

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

const tmpImageName = "gleaner_local_test_image"

func buildDockerImage() error {
	// run docker build to create the gleaner image
	buildCmd := exec.Command("docker", "build", "--build-arg", "BINARY_NAME=gleaner", ".", "-t", tmpImageName)
	buildCmd.Dir = "../.."
	output, err := buildCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed building gleaner image: %w\n%s", err, output)
	}
	return nil
}

// Create a handle to a struct which allows for easy handling of the minio container
func NewGleanerContainer(cmd string, networkName string) (GleanerContainer, error) {

	ctx := context.Background()

	if err := buildDockerImage(); err != nil {
		return GleanerContainer{}, fmt.Errorf("failed building gleaner image: %w", err)
	}

	req := testcontainers.ContainerRequest{
		Image: tmpImageName,
		// wait for the crawl to finish so our tests operate on the full data
		WaitingFor: wait.ForExit(),
		Cmd:        strings.Split(cmd, " "),
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
