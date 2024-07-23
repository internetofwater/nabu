BINARY := nabu
DOCKERVER :=`cat VERSION`
.DEFAULT_GOAL := nabu
VERSION :=`cat VERSION`
   
nabu:
	cd cmd/$(BINARY); \
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 env go build -o $(BINARY) 

nabu.m2.linux:
	cd cmd/$(BINARY) ; \
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 env go build  -o $(BINARY)_m2_linux;\
    cp $(BINARY)_m2_linux ../../

docker:
	podman build  --tag="fils/nabu:$(VERSION)"  --file=./build/Dockerfile .

dockerpush:
	podman push localhost/fils/nabu:$(VERSION) fils/nabu:$(VERSION)
	podman push localhost/fils/nabu:$(VERSION) fils/nabu:latest

publish:
	docker tag fils/nabu:$(VERSION) fils/nabu:latest
	docker push fils/nabu:$(VERSION) ; \
	docker push fils/nabu:latest

releases: nabu nabu.m2.linux docker dockerpush publish
