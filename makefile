#############################################################################
# This file contains helper scripts for nabu
# they are not required for building or running nabu
# and are just intended for local development


# Generate a csv report of the http requests (requires R to be installed)
report:
	NABU_PROFILING=true go test ./... -count=1
	Rscript scripts/clean_report.r ../http_trace.csv

# clean up trace/profiling artifacts
clean:
	find . -type f -name "http_trace.csv" -delete
	find . -type f -name "coverage.out" -delete

# list the top 8 cyclomatic complexity in the repo
# requires gocyclo to be installed.
# aiming for cyclomatic complexity of 12 or less is a good rule of thumb
cyclo:
	gocyclo -top 8 .