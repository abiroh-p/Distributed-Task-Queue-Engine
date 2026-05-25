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
