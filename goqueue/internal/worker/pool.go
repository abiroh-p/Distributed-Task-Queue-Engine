package worker

import (
    "context"
    "sync"
    "fmt"
    "golang.org/x/sync/semaphore"
    "github.com/rs/zerolog/log"
    "github.com/abishekP101/goqueue/internal/broker"
    "github.com/abishekP101/goqueue/internal/store"
    "github.com/abishekP101/goqueue/internal/events"

)

type Pool struct {
    broker     *broker.Broker
    store      *store.Store
    workerID   string
    maxWorkers int64
    sem        *semaphore.Weighted
    wg         sync.WaitGroup
    publisher   EventPublisher

}
type EventPublisher interface {
    Publish(event events.Event)
}

func New(b *broker.Broker, s *store.Store, workerID string, maxWorkers int64, publisher EventPublisher) *Pool {
    return &Pool{
        broker:     b,
        store:      s,
        workerID:   workerID,
        maxWorkers: maxWorkers,
        sem:        semaphore.NewWeighted(maxWorkers),
        publisher:  publisher,

    }
}
func (p *Pool) Run(ctx context.Context) {
    log.Info().Str("worker_id", p.workerID).Msg("worker pool started")

    for {
        select {
        case <-ctx.Done():
            log.Info().Msg("worker pool stopping, draining in-flight jobs...")
            p.wg.Wait()
            log.Info().Msg("all jobs drained, worker pool stopped")
            return
        default:
        }

        msg, err := p.broker.Consume(ctx, p.workerID)
        if err != nil {
            log.Error().Err(err).Msg("consume error")
            continue
        }
        if msg == nil {
            continue
        }

        if err := p.sem.Acquire(ctx, 1); err != nil {
            log.Error().Err(err).Msg("semaphore acquire failed")
            return
        }

        p.wg.Add(1)
        go p.process(ctx, msg)
    }
}

func (p *Pool) process(ctx context.Context, msg *broker.Message) {
    defer p.wg.Done()
    defer p.sem.Release(1)

    log.Info().
        Str("job_id", msg.JobID).
        Str("worker_id", p.workerID).
        Msg("processing job")

    // publish running event
    p.publisher.Publish(events.Event{
        Type:     "job.running",
        JobID:    msg.JobID,
        Status:   "running",
        WorkerID: p.workerID,
    })

    if err := p.store.AcquireLease(ctx, msg.JobID, p.workerID); err != nil {
        log.Error().Err(err).Str("job_id", msg.JobID).Msg("failed to acquire lease")
        return
    }

    err := p.execute(msg)
    if err != nil {
        log.Error().Err(err).Str("job_id", msg.JobID).Msg("job failed")

        p.publisher.Publish(events.Event{
            Type:     "job.failed",
            JobID:    msg.JobID,
            Status:   "failed",
            WorkerID: p.workerID,
        })

        if storeErr := p.store.UpdateStatus(ctx, msg.JobID, "failed"); storeErr != nil {
            log.Error().Err(storeErr).Msg("failed to update status")
        }
        if storeErr := p.store.IncrementAttempts(ctx, msg.JobID); storeErr != nil {
            log.Error().Err(storeErr).Msg("failed to increment attempts")
        }
        return
    }

    if err := p.broker.Ack(ctx, msg.Stream, msg.StreamID); err != nil {
        log.Error().Err(err).Str("job_id", msg.JobID).Msg("failed to ack")
        return
    }

    if err := p.store.UpdateStatus(ctx, msg.JobID, "succeeded"); err != nil {
        log.Error().Err(err).Str("job_id", msg.JobID).Msg("failed to update status")
        return
    }

    p.publisher.Publish(events.Event{
        Type:     "job.succeeded",
        JobID:    msg.JobID,
        Status:   "succeeded",
        WorkerID: p.workerID,
    })

    log.Info().Str("job_id", msg.JobID).Msg("job completed successfully")
}

func (p *Pool) execute(msg *broker.Message) error {
    // TODO: route to registered job handlers by type
    fmt.Printf("executing job %s with payload: %s\n", msg.JobID, msg.Payload)
    return nil
}