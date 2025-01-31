package testhelpers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GleanerContainer struct {
	Container testcontainers.Container
}

// Create a handle to a struct which allows for easy handling of the minio container
func NewGleanerContainer(configPath string, cmd []string, networkName string) (GleanerContainer, error) {
	ctx := context.Background()

	fullCmd := append([]string{"--cfg", "/app/gleanerconfig.yaml"}, cmd...)

	req := testcontainers.ContainerRequest{
		Image: "internetofwater/gleaner:latest",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      configPath,
				ContainerFilePath: "/app/gleanerconfig.yaml",
			},
		},
		// Wait for the harvest to finish
		WaitingFor: wait.ForExit(),
		Cmd:        fullCmd,
		Networks:   []string{networkName},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return GleanerContainer{}, fmt.Errorf("generic container: %w", err)
	}

	return GleanerContainer{Container: genericContainer}, nil

}
