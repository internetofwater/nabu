This directory contains all code and IAC deployment files that are required to run the shacl validation services; these are generally used for other services like the docs playgrounds which need a live public endpoint to test against

To run this terraform code you can run 

`gcloud auth application-default login` to authenticate and then `terraform init && terraform apply` to deploy