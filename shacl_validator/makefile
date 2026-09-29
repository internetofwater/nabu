buildPython:
	docker build . --file Dockerfile_py --tag shacl_validator_grpc_py

runPython:
	docker build . --file Dockerfile_py --tag shacl_validator_grpc_py && \
	docker run --rm -it \
		-p 50051:50051 \
		-p 8000:8000 \
		shacl_validator_grpc_py

test:
	cd shacl_validator_grpc_py && \
	uv run pytest