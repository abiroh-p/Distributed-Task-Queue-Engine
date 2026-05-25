package main

import (
    "context"
    "os/signal"
    "syscall"

    "github.com/rs/zerolog/log"

    "github.com/abishekP101/goqueue/config"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/scheduler"
    "github.com/abishekP101/goqueue/internal/store"
)

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

    engine := scheduler.New(b, s)

    log.Info().Msg("scheduler starting")
    engine.Run(ctx)
}