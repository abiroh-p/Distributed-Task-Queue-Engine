package main

import (
    "context"
    "fmt"
    "net"
    "os/signal"
    "syscall"
    "github.com/rs/zerolog/log"
    "google.golang.org/grpc"
    "github.com/abishekP101/goqueue/config"
    "github.com/abishekP101/goqueue/internal/api"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer stop()

    // 1. config
    cfg := config.Load()
    log.Info().Str("grpc_port", cfg.GRPCPort).Msg("configuration loaded")

    // 2. store
    s, err := store.New(cfg.PostgresDSN)
    if err != nil {
        log.Fatal().Err(err).Msg("failed to connect to postgres")
    }
    log.Info().Msg("postgres connected")

    // 3. broker
    b := broker.New(cfg.RedisAddr)
    if err := b.CreateConsumerGroup(ctx); err != nil {
        log.Fatal().Err(err).Msg("failed to create consumer groups")
    }
    log.Info().Msg("redis broker ready")

    // 4. api server
    srv := api.New(b, s)
    log.Info().Msg("api server initialized")

    // 5. grpc listener
    lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
    if err != nil {
        log.Fatal().Err(err).Msg("failed to bind grpc port")
    }

    grpcSrv := grpc.NewServer()
    _ = srv // TODO: register gRPC handlers once proto is compiled

    log.Info().Str("port", cfg.GRPCPort).Msg("gRPC server listening")

    go func() {
        if err := grpcSrv.Serve(lis); err != nil {
            log.Fatal().Err(err).Msg("grpc serve error")
        }
    }()

    <-ctx.Done()
    log.Info().Msg("shutdown signal received")
    grpcSrv.GracefulStop()
    log.Info().Msg("server stopped cleanly")
}