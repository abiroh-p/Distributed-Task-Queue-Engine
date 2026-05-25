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
