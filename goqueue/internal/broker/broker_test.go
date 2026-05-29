package broker_test

import (
    "context"
    "testing"

    "github.com/alicebob/miniredis/v2"

    "github.com/abishekP101/goqueue/internal/broker"
)

func setupBroker(t *testing.T) (*broker.Broker, *miniredis.Miniredis) {
    t.Helper()
    mr, err := miniredis.Run()
    if err != nil {
        t.Fatalf("failed to start miniredis: %v", err)
    }
    b := broker.New(mr.Addr())
    return b, mr
}
func TestCreateConsumerGroup(t *testing.T) {
    b, mr := setupBroker(t)
    defer mr.Close()

    ctx := context.Background()
    err := b.CreateConsumerGroup(ctx)
    if err != nil {
        t.Fatalf("expected no error, got: %v", err)
    }

    // calling again should not error (BUSYGROUP handled)
    err = b.CreateConsumerGroup(ctx)
    if err != nil {
        t.Fatalf("expected no error on duplicate group creation, got: %v", err)
    }
}
func TestEnqueueAndConsume(t *testing.T) {
    b, mr := setupBroker(t)
    defer mr.Close()

    ctx := context.Background()

    if err := b.CreateConsumerGroup(ctx); err != nil {
        t.Fatalf("setup failed: %v", err)
    }

    err := b.Enqueue(ctx, 5, "test-job-001", `{"task":"hello"}`)
    if err != nil {
        t.Fatalf("enqueue failed: %v", err)
    }

    msg, err := b.Consume(ctx, "test-worker")
    if err != nil {
        t.Fatalf("consume failed: %v", err)
    }
    if msg == nil {
        t.Fatal("expected a message, got nil")
    }
    if msg.JobID != "test-job-001" {
        t.Errorf("expected job_id test-job-001, got %s", msg.JobID)
    }
    if msg.Payload != `{"task":"hello"}` {
        t.Errorf("unexpected payload: %s", msg.Payload)
    }
}
func TestAck(t *testing.T) {
    b, mr := setupBroker(t)
    defer mr.Close()

    ctx := context.Background()

    if err := b.CreateConsumerGroup(ctx); err != nil {
        t.Fatalf("setup failed: %v", err)
    }

    b.Enqueue(ctx, 5, "test-job-002", `{"task":"ack-test"}`)

    msg, err := b.Consume(ctx, "test-worker")
    if err != nil || msg == nil {
        t.Fatalf("consume failed: %v", err)
    }

    err = b.Ack(ctx, msg.Stream, msg.StreamID)
    if err != nil {
        t.Fatalf("ack failed: %v", err)
    }
}