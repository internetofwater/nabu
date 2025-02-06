FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

WORKDIR /app

# Explicitly download these files before the build so they can be cached
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go mod tidy && \
    go build -o nabu

# Update the schema files with the latest version
ADD https://schema.org/version/latest/schemaorg-current-https.jsonld /assets/schemaorg-current-https.jsonld
ADD https://schema.org/version/latest/schemaorg-current-http.jsonld /assets/schemaorg-current-http.jsonld

FROM alpine

WORKDIR /app
COPY --from=builder /app/nabu /app/nabu

ENTRYPOINT ["/app/nabu"]
