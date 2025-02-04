FROM golang:1.23

WORKDIR /app

COPY . .

RUN go mod tidy && \
    go build -o nabu

# Update the schema files with the latest version
ADD https://schema.org/version/latest/schemaorg-current-https.jsonld /assets/schemaorg-current-https.jsonld
ADD https://schema.org/version/latest/schemaorg-current-http.jsonld /assets/schemaorg-current-http.jsonld

ENTRYPOINT ["/app/nabu"]
