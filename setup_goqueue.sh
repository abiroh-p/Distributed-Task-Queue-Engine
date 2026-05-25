#!/usr/bin/env bash
set -e

# ─────────────────────────────────────────────
#  goqueue — folder structure bootstrap script
# ─────────────────────────────────────────────

PROJECT="goqueue"
MODULE="github.com/abishekP101/goqueue"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Setting up $PROJECT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── root ──────────────────────────────────────
mkdir -p $PROJECT
cd $PROJECT

# ── cmd ───────────────────────────────────────
mkdir -p cmd/server
mkdir -p cmd/worker
mkdir -p cmd/scheduler

# ── internal packages ──────────────────────────
mkdir -p internal/api
mkdir -p internal/broker
mkdir -p internal/worker
mkdir -p internal/scheduler
mkdir -p internal/store
mkdir -p internal/queue
mkdir -p internal/dashboard
mkdir -p internal/middleware

# ── proto ─────────────────────────────────────
mkdir -p proto/goqueue/v1

# ── db migrations ─────────────────────────────
mkdir -p migrations

# ── deploy ────────────────────────────────────
mkdir -p deploy/k8s
mkdir -p deploy/grafana/dashboards

# ── dashboard UI ──────────────────────────────
mkdir -p dashboard/static

# ── config ────────────────────────────────────
mkdir -p config

# ── scripts ───────────────────────────────────
mkdir -p scripts


# ── entrypoints ────────────────────────────────
cat > cmd/server/main.go << 'GOEOF'
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Println("goqueue API server starting...")
	// TODO: wire up api.NewServer, broker, store
	<-ctx.Done()
	log.Println("shutting down gracefully...")
}
GOEOF

cat > cmd/worker/main.go << 'GOEOF'
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Println("goqueue worker node starting...")
	// TODO: wire up worker.Pool, broker client
	<-ctx.Done()
	log.Println("worker draining in-flight jobs...")
}
GOEOF

cat > cmd/scheduler/main.go << 'GOEOF'
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log.Println("goqueue scheduler starting...")
	// TODO: wire up scheduler.Engine, store
	<-ctx.Done()
	log.Println("scheduler stopped.")
}
GOEOF


# ── placeholder files for internal packages ────
touch internal/api/server.go
touch internal/broker/broker.go
touch internal/worker/pool.go
touch internal/scheduler/engine.go
touch internal/store/store.go
touch internal/queue/priority.go
touch internal/dashboard/hub.go
touch internal/middleware/auth.go


# ── proto definition ───────────────────────────
cat > proto/goqueue/v1/job.proto << 'PROTOEOF'
syntax = "proto3";

package goqueue.v1;

option go_package = "github.com/abishekP101/goqueue/proto/goqueue/v1;goqueuev1";

service JobService {
  rpc EnqueueJob    (EnqueueJobRequest)    returns (EnqueueJobResponse);
  rpc GetJobStatus  (GetJobStatusRequest)  returns (GetJobStatusResponse);
  rpc CancelJob     (CancelJobRequest)     returns (CancelJobResponse);
}

message Job {
  string id          = 1;
  string queue       = 2;
  string payload     = 3;
  int32  priority    = 4;
  int64  run_at      = 5;  // unix timestamp; 0 = now
  int32  max_retries = 6;
  string status      = 7;
}

message EnqueueJobRequest  { Job job = 1; }
message EnqueueJobResponse { string job_id = 1; }

message GetJobStatusRequest  { string job_id = 1; }
message GetJobStatusResponse { Job job = 1; }

message CancelJobRequest  { string job_id = 1; }
message CancelJobResponse { bool cancelled = 1; }
PROTOEOF


# ── first migration ────────────────────────────
cat > migrations/000001_create_jobs.up.sql << 'SQLEOF'
CREATE TABLE IF NOT EXISTS jobs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue        TEXT        NOT NULL DEFAULT 'default',
    payload      JSONB       NOT NULL DEFAULT '{}',
    priority     INT         NOT NULL DEFAULT 5,
    status       TEXT        NOT NULL DEFAULT 'pending',
    attempts     INT         NOT NULL DEFAULT 0,
    max_retries  INT         NOT NULL DEFAULT 3,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    leased_at    TIMESTAMPTZ,
    leased_by    TEXT,
    completed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_status_run_at ON jobs(status, run_at);
CREATE INDEX idx_jobs_queue_priority ON jobs(queue, priority DESC);
SQLEOF

cat > migrations/000001_create_jobs.down.sql << 'SQLEOF'
DROP TABLE IF EXISTS jobs;
SQLEOF


# ── docker compose ────────────────────────────
cat > deploy/docker-compose.yml << 'YAMLEOF'
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: goqueue
      POSTGRES_PASSWORD: goqueue
      POSTGRES_DB: goqueue
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --appendonly yes

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafdata:/var/lib/grafana

volumes:
  pgdata:
  grafdata:
YAMLEOF

cat > deploy/prometheus.yml << 'YAMLEOF'
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: goqueue-server
    static_configs:
      - targets: ["host.docker.internal:9091"]
  - job_name: goqueue-worker
    static_configs:
      - targets: ["host.docker.internal:9092"]
YAMLEOF


# ── config ────────────────────────────────────
cat > config/config.go << 'GOEOF'
package config

import "os"

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	GRPCPort      string
	HTTPPort      string
	MetricsPort   string
	WorkerCount   int
}

func Load() Config {
	return Config{
		PostgresDSN: getEnv("POSTGRES_DSN", "postgres://goqueue:goqueue@localhost:5432/goqueue?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		GRPCPort:    getEnv("GRPC_PORT", "50051"),
		HTTPPort:    getEnv("HTTP_PORT", "8080"),
		MetricsPort: getEnv("METRICS_PORT", "9091"),
		WorkerCount: 10,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
GOEOF


# ── Makefile ───────────────────────────────────
cat > Makefile << 'MAKEEOF'
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
MAKEEOF


# ── .gitignore ────────────────────────────────
cat > .gitignore << 'GIEOF'
bin/
*.env
.env
*.log
*.test
/tmp/
GIEOF


# ── README ────────────────────────────────────
cat > README.md << 'MDEOF'
# goqueue

A production-grade distributed task queue engine built in Go.

## Features
- Priority queues (high / default / low) via Redis Streams
- At-least-once delivery with consumer groups + explicit ACK
- Worker lease + heartbeat with automatic stall recovery
- Delayed jobs, cron scheduling, exponential-backoff retry
- Dead-letter queue (DLQ) with dashboard inspector
- gRPC API with REST transcoding (grpc-gateway)
- Real-time WebSocket dashboard
- Prometheus metrics + Grafana dashboards

## Quick start
```bash
make up        # start postgres + redis + prometheus + grafana
make migrate   # run DB migrations
go run ./cmd/server    # API gateway  :8080 gRPC :50051
go run ./cmd/worker    # worker node
go run ./cmd/scheduler # scheduler
```

## Architecture
See docs/architecture.md (coming soon)
MDEOF


# ── go mod init ────────────────────────────────
go mod init $MODULE 2>/dev/null || true


echo ""
echo "✓ Folder structure created"
echo "✓ Entrypoints scaffolded (cmd/)"
echo "✓ Proto definition written"
echo "✓ First DB migration ready"
echo "✓ Docker Compose wired (postgres, redis, prometheus, grafana)"
echo "✓ Makefile ready"
echo "✓ go.mod initialized"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Next → open the folder in VS Code / your editor"
echo "  Then run:  cd $PROJECT && make up"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "════════════════════════════════════════════"
echo "  🔔  COMMIT REMINDER"
echo "  git init && git add . && git commit -m 'chore: initial project scaffold'"
echo "  git remote add origin https://github.com/abiroh-p/goqueue.git"
echo "  git push -u origin main"
echo "════════════════════════════════════════════"
echo ""