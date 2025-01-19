package testhelpers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
)

type GleanerContainer struct {
	Container testcontainers.Container
	Context   context.Context
}

func StartGleanerContainer(cmd string) (*GleanerContainer, error) {
	ctx := context.Background()

	// Create a container request for the Gleaner image
	containerReq := testcontainers.ContainerRequest{
		Image: "internetofwater/gleaner:latest",
		Name:  "gleanerTestContainer",
		Cmd:   []string{cmd}, // Command to run in the container
	}

	// Start the container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start Gleaner container: %w", err)
	}

	return &GleanerContainer{
		Container: container,
		Context:   ctx,
	}, nil
}
