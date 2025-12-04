buildRust:
	docker build . --file Dockerfile_rs

runRust:
	docker build . --file Dockerfile_rs --tag shacl_validator_grpc_rs && \
	docker run --rm -it -v /tmp:/tmp shacl_validator_grpc_rs

buildPython:
	docker build . --file Dockerfile_py

runPython:
	docker build . --file Dockerfile_py --tag shacl_validator_grpc_py && \
	docker run --rm -it \
		-p 50051:50051 \
		-p 8000:8000 \
		shacl_validator_grpc_py

test:
	cd shacl_validator_grpc_py && \
	uv run pytest && \
	cd ../shacl_validator_grpc_rs && cargo test