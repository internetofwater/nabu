#############################################################################
# This file contains helper scripts for nabu
# they are not required for building or running nabu
# and are just intended for local development

# Generate a csv report of the http requests (requires R to be installed)
report:
	Rscript scripts/clean_report.r ../http_trace.csv

# clean up trace/profiling/build artifacts
clean:
	find . -type f -name "http_trace.csv" -delete
	find . -type f -name "coverage.out" -delete
	rm -f nabu

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