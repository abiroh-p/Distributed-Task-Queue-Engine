<div align="center">

# goqueue

**A production-grade distributed task queue engine built in Go**

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://go.dev)
[![Redis](https://img.shields.io/badge/Redis-Streams-DC382D?style=flat&logo=redis)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql)](https://postgresql.org)
[![gRPC](https://img.shields.io/badge/gRPC-protobuf-244c5a?style=flat)](https://grpc.io)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat)](LICENSE)

*Think Celery or BullMQ, but built from scratch in Go*

</div>

---

## What is this?

goqueue is a distributed background job processing system built from first principles in Go. It demonstrates production-level backend engineering: distributed coordination, at-least-once delivery guarantees, graceful shutdown, real-time observability, and Kubernetes-native autoscaling.

**Core design decisions:**
- **Redis Streams** over simple `LPUSH/RPOP`: consumer groups give at-least-once delivery with a Pending Entry List (PEL). Crashed workers don't lose jobs.
- **PostgreSQL as source of truth**: Redis handles speed, Postgres handles durability. Every job has an audit trail.
- **Worker lease + heartbeat**: workers stamp jobs with ownership and expiry. The scheduler reclaims stalled leases automatically.
- **Bounded goroutine pool**: semaphore-based concurrency limit prevents resource exhaustion under burst load.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                          │
│              Go SDK  ·  REST  ·  gRPC                   │
└───────────────────────────┬─────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│                   API Gateway (Go)                      │
│         gRPC + JWT auth · rate limiting · validation    │
└──────────────┬────────────────────────┬─────────────────┘
               │                        │
┌──────────────▼──────────┐  ┌──────────▼──────────────┐
│   Broker (Redis Streams) │  │    Scheduler (Go)        │
│  high · default · low    │  │  cron · retry · backoff  │
│  consumer groups · PEL   │  │  stall recovery          │
└──────────────┬───────────┘  └─────────────────────────┘
               │
┌──────────────▼───────────────────────────────────────┐
│              Worker Pool  ×N  (Go)                   │
│    goroutine pool · semaphore · lease · heartbeat    │
└──────┬────────────────────┬────────────────┬─────────┘
       │                    │                │
┌──────▼──────┐  ┌──────────▼──────┐  ┌─────▼───────────────┐
│ PostgreSQL  │  │     Redis       │  │  Prometheus + Loki   │
│  job store  │  │  queues · DLQ   │  │  metrics · logs      │
│  audit log  │  │  rate limit     │  │                      │
└─────────────┘  └─────────────────┘  └──────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────┐
│              Dashboard (Go + WebSocket)                  │
│    real-time job feed · queue depth · DLQ inspector      │
└──────────────────────────────────────────────────────────┘
```

---

## Features

**Queue mechanics**
- Priority queues : `high`, `default`, `low` via separate Redis Streams
- At-least-once delivery with consumer groups and explicit ACK
- Delayed job execution and cron scheduling
- Exponential backoff with jitter on retry (prevents thundering herd)
- Dead-letter queue (DLQ) with replay support

**Reliability**
- Worker lease + heartbeat pattern, stalled jobs auto-recovered by scheduler
- Graceful shutdown: drains in-flight jobs before exit (`sync.WaitGroup`)
- Bounded concurrency: semaphore-based goroutine pool
- Database-level mutex on lease acquisition (prevents race conditions)

**Observability**
- Structured JSON logging with `zerolog`
- Prometheus metrics exposed on `/metrics`
- Grafana dashboard pre-configured
- Real-time WebSocket event feed (job state changes pushed to dashboard)

**API**
- gRPC with protobuf (`EnqueueJob`, `GetJobStatus`, `CancelJob`)
- JWT authentication via unary interceptor
- gRPC reflection enabled for tooling

---

## Quick start

**Prerequisites:** Go 1.22+, Docker

```bash
# 1. clone
git clone https://github.com/abiroh-p/Distributed-Task-Queue-Engine
cd Distributed-Task-Queue-Engine/goqueue

# 2. start infrastructure
docker compose -f deploy/docker-compose.yml up -d

# 3. run migrations
migrate -path ./migrations \
  -database "postgres://goqueue:goqueue@localhost:5432/goqueue?sslmode=disable" up

# 4. start all three components (separate terminals)
go run ./cmd/server      # gRPC API  :50051  HTTP :8080
go run ./cmd/worker      # worker node
go run ./cmd/scheduler   # scheduler
```

---

## Enqueue a job

Generate a token:
```bash
go run scripts/gen_token.go
```

Call the gRPC API:
```bash
grpcurl -plaintext \
  -H "authorization: Bearer <token>" \
  -d '{"job": {"queue": "default", "payload": "{\"task\":\"hello\"}", "priority": 5, "max_retries": 3}}' \
  localhost:50051 goqueue.v1.JobService/EnqueueJob
```

Response:
```json
{ "jobId": "927b3985-bdad-4949-932b-af9d85440910" }
```

---

## Job lifecycle

```
pending → running → succeeded
                 ↘ failed → retry (exp backoff + jitter)
                          ↘ dead (max retries exceeded → DLQ)
```

---

## Project structure

```
goqueue/
├── cmd/
│   ├── server/       # API gateway entrypoint
│   ├── worker/       # worker node entrypoint
│   └── scheduler/    # scheduler entrypoint
├── internal/
│   ├── api/          # gRPC handlers
│   ├── broker/       # Redis Streams abstraction
│   ├── dashboard/    # WebSocket hub
│   ├── events/       # shared event types
│   ├── middleware/   # JWT auth interceptor
│   ├── queue/        # DLQ promote + replay
│   ├── scheduler/    # stall recovery + backoff engine
│   ├── store/        # PostgreSQL repository
│   └── worker/       # goroutine pool + lease
├── proto/goqueue/v1/ # protobuf definitions
├── migrations/       # golang-migrate SQL files
├── deploy/           # Docker Compose + K8s manifests
├── scripts/          # dev utilities
└── config/           # environment-based config
```

---

## Stack

| Layer | Technology | Reason |
|---|---|---|
| Language | Go 1.22 | goroutines, fast startup, strong stdlib |
| Queue broker | Redis Streams | consumer groups, PEL, at-least-once |
| Job store | PostgreSQL 16 | ACID, audit log, complex queries |
| API | gRPC + protobuf | type-safe, REST compat via grpc-gateway |
| Auth | JWT (HS256) | stateless, per-client identity |
| Migrations | golang-migrate | versioned, reproducible |
| Observability | Prometheus + Grafana + Loki | industry standard |
| Logging | zerolog | structured JSON, zero allocation |
| Container | Docker Compose → Kubernetes | local dev + production |

---

## Testing

```bash
# all tests with race detector
go test ./... -race

# broker integration tests (miniredis — no Docker needed)
go test ./internal/broker/... -v

# worker pool tests (mock store + publisher)
go test ./internal/worker/... -v -race
```

---

## Key engineering concepts demonstrated

**At-least-once delivery** — Redis Streams consumer groups keep messages in a Pending Entry List until explicitly ACKed. If a worker crashes mid-job, the message stays in PEL and the scheduler reclaims it.

**Worker lease pattern** — workers write a lease row with a timestamp on job pickup. The scheduler sweeps for leases older than 60s and requeues those jobs. Same pattern used by Kubernetes controllers.

**Thundering herd prevention** — retry backoff uses `min(base × 2^attempts + jitter, maxDelay)`. Random jitter spreads retries across time so 100 failing jobs don't all hammer the system simultaneously.

**Dependency inversion** — worker pool depends on `Store` and `EventPublisher` interfaces, not concrete types. Enables mock-based testing without infrastructure.

**Graceful shutdown** — `signal.NotifyContext` catches SIGTERM, cancels the root context, worker pool drains in-flight jobs via `sync.WaitGroup` before exit.

---

## Roadmap

- [ ] KEDA autoscaling — scale worker pods on Redis stream length
- [ ] Job handler registry — route jobs to typed handlers by queue name
- [ ] grpc-gateway — REST transcoding for non-gRPC clients
- [ ] Prometheus metrics — queue depth, job latency, worker utilization
- [ ] Dashboard UI — real-time HTML dashboard served over WebSocket

---

<div align="center">

Built by [Abishek](https://github.com/abiroh-p) · B.Tech Computer Engineering

</div>
