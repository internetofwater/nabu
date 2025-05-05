// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// A struct to represent the minio container
type MinioContainer struct {
	// the container itself. used for testcontainer cleanup
	Container *testcontainers.Container
	Hostname  string
	APIPort   int
	UIPort    int
	// the minio client for interacting with this client. This uses our custom
	// client with the helper methods we need for nabu
	ClientWrapper *MinioClientWrapper
}

type MinioContainerConfig struct {
	// the username for the minio container
	Username string
	// the password for the minio container
	Password string
	// the name of the default bucket in minio for all operations
	DefaultBucket string
	// the name of the container being spun up
	ContainerName string
	// the network that the container uses; leave blank to use default
	Network string
}

// Create a default minio container with default credentials, mainly for testing
func NewDefaultMinioContainer() (MinioContainer, error) {
	container, err := NewMinioContainerFromConfig(MinioContainerConfig{
		Username:      "minioadmin",
		Password:      "minioadmin",
		DefaultBucket: "iow",
	})
	if err != nil {
		return container, err
	}
	if err := container.ClientWrapper.MakeDefaultBucket(); err != nil {
		return container, err
	}
	return container, err
}

// Spin up a local minio container
func NewMinioContainerFromConfig(config MinioContainerConfig) (MinioContainer, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image: "minio/minio:latest",
		// expose the API with 9000 and the UI with 9001
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000"),
		Env: map[string]string{
			"MINIO_ROOT_USER":     config.Username,
			"MINIO_ROOT_PASSWORD": config.Password,
		},
		// We need to expose the console at 9001 to access the UI
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
	}

	if config.ContainerName != "" {
		req.Name = config.ContainerName
	}
	if config.Network != "" {
		req.Networks = []string{config.Network}
		req.NetworkAliases = map[string][]string{config.Network: {config.ContainerName}}
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return MinioContainer{}, fmt.Errorf("generic container: %w", err)
	}

	hostname, err := genericContainer.Host(ctx)
	if err != nil {
		return MinioContainer{}, fmt.Errorf("get hostname: %w", err)
	}

	apiPort, err := genericContainer.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return MinioContainer{}, fmt.Errorf("get api port: %w", err)
	}

	uiPort, err := genericContainer.MappedPort(ctx, "9001/tcp")
	if err != nil {
		return MinioContainer{}, fmt.Errorf("get ui port: %w", err)
	}

	url := fmt.Sprintf("%s:%d", hostname, apiPort.Int())

	mc, err := minio.New(url, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Username, config.Password, ""),
		Secure: false,
	})
	if err != nil {
		return MinioContainer{}, fmt.Errorf("minio client: %w", err)
	}

	return MinioContainer{
		Container:     &genericContainer,
		ClientWrapper: &MinioClientWrapper{Client: mc, DefaultBucket: config.DefaultBucket},
		Hostname:      hostname,
		APIPort:       apiPort.Int(),
		UIPort:        uiPort.Int(),
	}, nil
}
