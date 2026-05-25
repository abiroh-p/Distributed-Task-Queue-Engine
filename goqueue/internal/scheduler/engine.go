package scheduler

import (
    "context"
    "math"
    "math/rand"
    "time"
    "github.com/rs/zerolog/log"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
)

const (
    sweepInterval = 30 * time.Second
    leaseTimeout  = 60 * time.Second
    baseDelay     = 2 * time.Second
    maxDelay      = 5 * time.Minute
)

type Engine struct {
    broker *broker.Broker
    store  *store.Store
}

func New(b *broker.Broker, s *store.Store) *Engine {
    return &Engine{
        broker: b,
        store:  s,
    }
}

func (e *Engine) Run(ctx context.Context) {
    log.Info().Msg("scheduler started")

    ticker := time.NewTicker(sweepInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            log.Info().Msg("scheduler stopped")
            return
        case <-ticker.C:
            e.recoverStalledJobs(ctx)
        }
    }
}

func (e *Engine) recoverStalledJobs(ctx context.Context) {
    jobs, err := e.store.GetStalledJobs(ctx, leaseTimeout)
    if err != nil {
        log.Error().Err(err).Msg("failed to get stalled jobs")
        return
    }

    for _, job := range jobs {
        job := job // capture loop variable

        go func() {
            delay := e.backoff(job.Attempts)

            log.Warn().
                Str("job_id", job.ID).
                Int("attempts", job.Attempts).
                Dur("retry_in", delay).
                Msg("recovering stalled job")

            select {
            case <-time.After(delay):
                // continue
            case <-ctx.Done():
                return
            }

            if err := e.store.UpdateStatus(ctx, job.ID, "pending"); err != nil {
                log.Error().
                    Err(err).
                    Str("job_id", job.ID).
                    Msg("failed to reset status")
                return
            }

            if err := e.broker.Enqueue(
                ctx,
                job.Priority,
                job.ID,
                job.Payload,
            ); err != nil {
                log.Error().
                    Err(err).
                    Str("job_id", job.ID).
                    Msg("failed to requeue job")
                return
            }

            log.Info().
                Str("job_id", job.ID).
                Msg("stalled job requeued")
        }()
    }
}

func (e *Engine) backoff(attempts int) time.Duration {
    exp := math.Pow(2, float64(attempts))
    delay := float64(baseDelay) * exp

    jitter := rand.Float64() * float64(baseDelay)
    delay += jitter

    if delay > float64(maxDelay) {
        delay = float64(maxDelay)
    }

    return time.Duration(delay)
}