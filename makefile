#############################################################################
# This file contains development commands
# they are not required for running
# and are just intended for local development/testing

# clean up trace/profiling/build artifacts
clean:
	find . -type f -name "http_trace.csv" -delete
	find . -type f -name "coverage.out" -delete
	rm -f __debug_bin*
	rm -f nabu
	rm -f gleaner

# list the top 8 cyclomatic complexity in the repo
# requires gocyclo to be installed.
# aiming for cyclomatic complexity of 12 or less is a good rule of thumb
cyclo:
	gocyclo -top 8 .

# Build nabu as a docker image
dockerBuild:
	docker build --build-arg BINARY_NAME=nabu .

# Generate coverage report and visualize it in a browser
coverage:
	go test ./... -coverprofile coverage.out
	go tool cover -html=coverage.out

# Check for deadcode in the project
deadcode:
	deadcode -test ./... 

# test with gotestsum, a helpful wrapper for go test
test:
	gotestsum --max-fails 1

lint:
	golangci-lint run

# run tests and print the slowest tests in the project
slowest:	
	gotestsum --jsonfile /tmp/json.log
	gotestsum tool slowest --jsonfile /tmp/json.log

# Check for max tcp connections to ensure no throttling
checkMaxTcpConnectionsPerProcess:
	ulimit -n 

tools:
	go install gotest.tools/gotestsum@latest

build_for_debug:
	go build -gcflags="all=-N -l" -o nabu ./cmd/nabu

init_sandbox_rules:
	sbx kit add claude-nabu ./dev-kit/