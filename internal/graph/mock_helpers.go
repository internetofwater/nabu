package graph

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GraphDBContainer struct {
	mappedPort int
	Container  *testcontainers.Container
	Client     GraphDbClient
}

// Spin up a local graphdb container and the associated client
func NewGraphDBContainer() (GraphDBContainer, error) {

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "khaller/graphdb-free",
		ExposedPorts: []string{"7200/tcp"},
		// We use a regex here since graphdb adds additional context info at the
		// start of the log message like the date / time
		WaitingFor: wait.ForLog(".*Started GraphDB in workbench mode at port 7200").AsRegexp(),
	}
	graphdbC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
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
	client := GraphDbClient{Endpoint: "http://" + host + ":" + port.Port()}
	return GraphDBContainer{Client: client, mappedPort: port.Int(), Container: &graphdbC}, nil
}
