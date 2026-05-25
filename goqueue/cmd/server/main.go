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
