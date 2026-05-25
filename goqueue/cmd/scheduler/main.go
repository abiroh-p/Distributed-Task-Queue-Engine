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
