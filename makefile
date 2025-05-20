#############################################################################
# This file contains development commands
# they are not required for running
# and are just intended for local development/testing

protoc:
	protoc --proto_path=. --go_out=. --go-grpc_out=. shacl_validator_grpc/proto/shacl_validator.proto

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

# Build gleaner as a docker image
dockerGleaner:
	docker build --build-arg BINARY_NAME=gleaner .

# Build nabu as a docker image
dockerNabu:
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
	gotestsum

# run tests and print the slowest tests in the project
slowest:	
	gotestsum --jsonfile /tmp/json.log
	gotestsum tool slowest --jsonfile /tmp/json.log