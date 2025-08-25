buildRust:
	docker build . --file Dockerfile_rs

buildPython:
	docker build . --file Dockerfile_py

test:
	cd shacl_validator_grpc_py && uv run pytest && cd ../shacl_validator_grpc_rs && cargo test