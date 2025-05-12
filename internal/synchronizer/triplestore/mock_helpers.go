// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package triplestore

import (
	"context"
	"fmt"

	"github.com/internetofwater/nabu/internal/config"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GraphDBContainer struct {
	mappedPort int
	Container  *testcontainers.Container
	Client     GraphDbClient
}

// Spin up a local graphdb container and the associated client
func NewGraphDBContainer(repositoryName string, configPath string) (GraphDBContainer, error) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "khaller/graphdb-free",
		Name:         "graphdbTestcontainer", // the name used for ryuk cleanup
		ExposedPorts: []string{"7200/tcp"},
		// We use a regex here since graphdb adds additional context info at the
		// start of the log message like the date / time
		WaitingFor: wait.ForLog(".*Started GraphDB in workbench mode at port 7200").AsRegexp(),
	}
	graphdbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})

	if err != nil {
		return GraphDBContainer{}, err
	}
	// 7200 is the default http endpoint
	port, err := graphdbC.MappedPort(ctx, "7200/tcp")

	if err != nil {
		return GraphDBContainer{}, err
	}

	host, err := graphdbC.Host(ctx)

	if err != nil {
		return GraphDBContainer{}, err
	}
	sparqlConfig := config.SparqlConfig{
		Endpoint:     "http://" + host + ":" + port.Port(),
		Authenticate: false,
		Username:     "",
		Password:     "",
	}
	client := GraphDbClient{
		SparqlConf:         sparqlConfig,
		BaseUrl:            fmt.Sprintf("http://%s:%s", host, port.Port()),
		BaseRepositoryUrl:  fmt.Sprintf("http://%s:%s/repositories/%s", host, port.Port(), repositoryName),
		BaseRESTUrl:        fmt.Sprintf("http://%s:%s/rest", host, port.Port()),
		BaseSparqlQueryUrl: fmt.Sprintf("http://%s:%s/repositories/%s/statements", host, port.Port(), repositoryName),
	}

	err = client.CreateRepositoryIfNotExists(configPath)
	if err != nil {
		return GraphDBContainer{}, fmt.Errorf("failed to create repository when initializing graphdb container: %w", err)
	}

	return GraphDBContainer{Client: client, mappedPort: port.Int(), Container: &graphdbC}, nil
}
