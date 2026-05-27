package queue

import (
    "context"
    "fmt"

    "github.com/rs/zerolog/log"

    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
)

const DLQStream = "goqueue:stream:dlq"

type DLQ struct {
    broker *broker.Broker
    store  *store.Store
}

func NewDLQ(b *broker.Broker, s *store.Store) *DLQ {
    return &DLQ{
        broker: b,
        store:  s,
    }
}

func (d *DLQ) Promote(ctx context.Context, job store.Job) error {
    if job.Attempts < job.MaxRetries {
        return fmt.Errorf("job %s has not exhausted retries", job.ID)
    }

    if err := d.broker.AddToDLQ(ctx, job.ID, job.Queue, job.Payload, "max retries exceeded"); err != nil {
        return fmt.Errorf("promoting to dlq: %w", err)
    }

    if err := d.store.UpdateStatus(ctx, job.ID, "dead"); err != nil {
        return fmt.Errorf("marking job dead: %w", err)
    }

    log.Warn().
        Str("job_id", job.ID).
        Int("attempts", job.Attempts).
        Msg("job promoted to DLQ")

    return nil
}

// NOTE: dual-write risk — if process crashes between UpdateStatus and Enqueue,
// the scheduler's stall recovery will requeue the pending job automatically.
func (d *DLQ) Replay(ctx context.Context, jobID string) error {
    job, err := d.store.GetJobByID(ctx, jobID)
    if err != nil {
        return fmt.Errorf("fetching job for replay: %w", err)
    }

    if job.Status != "dead" {
        return fmt.Errorf("job %s is not in dead status, current status: %s", jobID, job.Status)
    }

    if err := d.store.UpdateStatus(ctx, jobID, "pending"); err != nil {
        return fmt.Errorf("resetting job status: %w", err)
    }

    if err := d.broker.Enqueue(ctx, job.Priority, job.ID, job.Payload); err != nil {
        return fmt.Errorf("replaying job to queue: %w", err)
    }

    log.Info().
        Str("job_id", jobID).
        Msg("dead job replayed to queue")

    return nil
}