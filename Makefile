.PHONY: fmt vet test race build clean docker-up docker-down

fmt:
go fmt ./...

vet:
go vet ./...

test:
go test ./...

race:
go test -race ./...

build:
go build ./...

clean:
go clean ./...

docker-up:
docker compose up -d

docker-down:
docker compose down

all: fmt vet test build
