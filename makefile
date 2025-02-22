# Generate a csv report of the http requests (requires R to be installed)
report:
	NABU_PROFILING=true go test ./... -count=1
	Rscript scripts/clean_report.r ../http_trace.csv
