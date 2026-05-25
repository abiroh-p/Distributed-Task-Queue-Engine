package api

import (
    "context"
    "fmt"
    "time"
    "github.com/google/uuid"
    "github.com/rs/zerolog/log"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
)

type Server struct {
    broker *broker.Broker
    store  *store.Store
}

func New(b *broker.Broker, s *store.Store) *Server {
    return &Server{
        broker: b,
        store:  s,
    }
}

type EnqueueRequest struct {
    Queue      string
    Payload    string
    Priority   int
    MaxRetries int
    RunAt      time.Time
}

type EnqueueResponse struct {
    JobID string
}

func (s *Server) validate(req EnqueueRequest) error {
    if req.Payload == "" {
        return fmt.Errorf("payload is required")
    }
    if req.Queue == "" {
        return fmt.Errorf("queue name is required")
    }
    if req.MaxRetries < 0 {
        return fmt.Errorf("max_retries cannot be negative")
    }
    return nil
}

func clampPriority(p int) int {
    if p < 1 {
        return 1
    }
    if p > 10 {
        return 10
    }
    return p
}


func (s *Server) EnqueueJob(ctx context.Context, req EnqueueRequest) (*EnqueueResponse, error) {
    if err := s.validate(req); err != nil {
        return nil, fmt.Errorf("validation failed: %w", err)
    }

    job := store.Job{
        ID:         uuid.New().String(),
        Queue:      req.Queue,
        Payload:    req.Payload,
        Priority:   clampPriority(req.Priority),
        MaxRetries: req.MaxRetries,
        RunAt:      req.RunAt,
    }

    if job.RunAt.IsZero() {
        job.RunAt = time.Now()
    }

    if err := s.store.CreateJob(ctx, job); err != nil {
        return nil, fmt.Errorf("persisting job: %w", err)
    }

    if err := s.broker.Enqueue(ctx, job.Priority, job.ID, job.Payload); err != nil {
        return nil, fmt.Errorf("enqueueing job: %w", err)
    }

    log.Info().
        Str("job_id", job.ID).
        Str("queue", job.Queue).
        Int("priority", job.Priority).
        Msg("job enqueued")

    return &EnqueueResponse{JobID: job.ID}, nil
}