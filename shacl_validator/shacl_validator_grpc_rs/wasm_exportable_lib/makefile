include .env
export

wasm:
	wasm-pack build --target web

clean:
	rm -rf ./pkg

push_to_ghcr:
	 oras push ghcr.io/internetofwater/geoconnex_shacl_validator_wasm:latest ./pkg:application/octet-stream --username internetofwater --password ${GHCR_TOKEN} --annotation "org.opencontainers.image.source=https://github.com/internetofwater/nabu" 