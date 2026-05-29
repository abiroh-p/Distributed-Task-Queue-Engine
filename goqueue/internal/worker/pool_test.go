package worker_test

import (
    "context"
    "sync"
    "testing"
    "time"

    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/events"
    "github.com/abishekP101/goqueue/internal/worker"

    "github.com/alicebob/miniredis/v2"
)

// mock publisher
type mockPublisher struct {
    mu     sync.Mutex
    events []events.Event
}

func (m *mockPublisher) Publish(e events.Event) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.events = append(m.events, e)
}

func (m *mockPublisher) Published() []events.Event {
    m.mu.Lock()
    defer m.mu.Unlock()
    return append([]events.Event{}, m.events...)
}

func setupPool(t *testing.T) (*worker.Pool, *broker.Broker, *mockPublisher, *miniredis.Miniredis) {
    t.Helper()

    mr, err := miniredis.Run()
    if err != nil {
        t.Fatalf("miniredis: %v", err)
    }

    b := broker.New(mr.Addr())
    ctx := context.Background()
    if err := b.CreateConsumerGroup(ctx); err != nil {
        t.Fatalf("consumer group: %v", err)
    }

    pub := &mockPublisher{}
    pool := worker.New(b, &mockStore{}, "test-worker", 2, pub)

    return pool, b, pub, mr
}

type mockStore struct{}

func (m *mockStore) AcquireLease(ctx context.Context, jobID, workerID string) error {
    return nil
}
func (m *mockStore) UpdateStatus(ctx context.Context, jobID, status string) error {
    return nil
}
func (m *mockStore) IncrementAttempts(ctx context.Context, jobID string) error {
    return nil
}


func TestPoolProcessesJob(t *testing.T) {
    pool, b, pub, mr := setupPool(t)
    defer mr.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // start pool in background
    done := make(chan struct{})
    go func() {
        defer close(done)
        pool.Run(ctx)
    }()

    // enqueue a job
    err := b.Enqueue(context.Background(), 5, "test-job-001", `{"task":"test"}`)
    if err != nil {
        t.Fatalf("enqueue failed: %v", err)
    }

    // wait for events with timeout
    deadline := time.After(5 * time.Second)
    for {
        select {
        case <-deadline:
            t.Fatal("timed out waiting for job to be processed")
        default:
            published := pub.Published()
            for _, e := range published {
                if e.JobID == "test-job-001" && e.Status == "succeeded" {
                    // job processed successfully
                    cancel()
                    <-done
                    return
                }
            }
            time.Sleep(50 * time.Millisecond)
        }
    }
}