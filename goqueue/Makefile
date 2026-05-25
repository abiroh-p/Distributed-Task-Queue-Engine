.PHONY: up down migrate proto lint test build

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

migrate:
	migrate -path ./migrations -database "${POSTGRES_DSN}" up

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/goqueue/v1/job.proto

lint:
	golangci-lint run ./...

test:
	go test ./... -race -count=1

build:
	go build -o bin/server    ./cmd/server
	go build -o bin/worker    ./cmd/worker
	go build -o bin/scheduler ./cmd/scheduler
