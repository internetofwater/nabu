# Generate a csv report of the http requests (requires R to be installed)
report:
	go test ./... -count=1
	Rscript scripts/clean_report.r internal/synchronizer/http_trace.csv
