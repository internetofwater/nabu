package objects

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
	// the url to the container http endpoint
	mappedHttpUrl string
	// the url to the container's ui. Useful for debugging if the container is paused
	mappedUIUrl string
	// the container itself. used for testcontainer cleanup
	Container *testcontainers.Container
	// the minio client for interacting with this client. This uses our custom
	// client with the helper methods we need for nabu
	ClientWrapper *MinioClientWrapper
}

func getAPIURL(container *testcontainers.Container, ctx context.Context) (string, error) {
	host, err := (*container).Host(ctx)
	if err != nil {
		return "", err
	}

	api, err := (*container).MappedPort(ctx, "9000/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", host, api.Port()), nil
}

func getUIURL(container *testcontainers.Container, ctx context.Context) (string, error) {
	host, err := (*container).Host(ctx)
	if err != nil {
		return "", err
	}
	ui, err := (*container).MappedPort(ctx, "9001/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", host, ui.Port()), nil
}

type MinioContainerConfig struct {
	Username      string
	Password      string
	DefaultBucket string
	ContainerName string
}

// Spin up a local minio container
func NewMinioContainer(config MinioContainerConfig) (MinioContainer, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image: "minio/minio:latest",
		// expose the UI with 9001
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		WaitingFor:   wait.ForHTTP("/minio/health/live").WithPort("9000"),
		Env: map[string]string{
			"MINIO_ROOT_USER":     config.Username,
			"MINIO_ROOT_PASSWORD": config.Password,
		},
		Networks: []string{"nabu_test_network"},
		// We need to expose the console at 9001 to access the UI
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
	}

	if config.ContainerName != "" {
		req.Name = config.ContainerName
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	if err != nil {
		return MinioContainer{}, fmt.Errorf("generic container: %w", err)
	}

	minioContainer := MinioContainer{Container: &genericContainer}

	url, err := getAPIURL(minioContainer.Container, ctx)

	if err != nil {
		return MinioContainer{}, fmt.Errorf("get api url: %w", err)
	}

	mc, err := minio.New(url, &minio.Options{
		Creds:  credentials.NewStaticV4(config.Username, config.Password, ""),
		Secure: false,
	})
	if err != nil {
		return MinioContainer{}, fmt.Errorf("minio client: %w", err)
	}

	mappedUI, err := getUIURL(minioContainer.Container, ctx)
	if err != nil {
		return MinioContainer{}, fmt.Errorf("get ui url: %w", err)
	}

	return MinioContainer{
		Container:     &genericContainer,
		ClientWrapper: &MinioClientWrapper{Client: mc, DefaultBucket: config.DefaultBucket},
		mappedHttpUrl: url,
		mappedUIUrl:   mappedUI,
	}, nil
}
