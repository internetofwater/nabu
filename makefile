# Generate a csv report of the http requests (requires R to be installed)
report:
	go test ./... -count=1
	Rscript scripts/report.r	
