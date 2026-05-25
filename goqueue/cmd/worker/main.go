package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/google/uuid"
    "github.com/rs/zerolog/log"

    "github.com/abishekP101/goqueue/config"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
    "github.com/abishekP101/goqueue/internal/worker"
)

func workerID() string {
    if host, err := os.Hostname(); err == nil && host != "" {
        return host
    }
    return uuid.New().String()
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    cfg := config.Load()

    s, err := store.New(cfg.PostgresDSN)
    if err != nil {
        log.Fatal().Err(err).Msg("failed to connect to postgres")
    }
    log.Info().Msg("postgres connected")

    b := broker.New(cfg.RedisAddr)
    log.Info().Msg("broker connected")

    id := workerID()
    pool := worker.New(b, s, id, int64(cfg.WorkerCount))

    log.Info().Str("worker_id", id).Int("concurrency", cfg.WorkerCount).Msg("worker starting")

    pool.Run(ctx)
}