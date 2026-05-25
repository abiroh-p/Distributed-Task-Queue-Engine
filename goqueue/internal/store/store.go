package store

import (
    "database/sql"
    "fmt"
    "time"
	"context"
    _"github.com/lib/pq"
)

type Job struct {
    ID          string
    Queue       string
    Payload     string
    Priority    int
    Status      string
    Attempts    int
    MaxRetries  int
    RunAt       time.Time
    LeasedAt    *time.Time
    LeasedBy    *string
    CompletedAt *time.Time
    CreatedAt   time.Time
}

type Store struct {
    db *sql.DB
}

func New(dsn string) (*Store, error) {
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        return nil, fmt.Errorf("opening db: %w", err)
    }
    if err := db.Ping(); err != nil {
        return nil, fmt.Errorf("pinging db: %w", err)
    }
    return &Store{db: db}, nil
}

func (s *Store) CreateJob(ctx context.Context, job Job) error {
    query := `
        INSERT INTO jobs (id, queue, payload, priority, status, max_retries, run_at)
        VALUES ($1, $2, $3, $4, 'pending', $5, $6)
    `
    _, err := s.db.ExecContext(ctx, query,
        job.ID,
        job.Queue,
        job.Payload,
        job.Priority,
        job.MaxRetries,
        job.RunAt,
    )
    if err != nil {
        return fmt.Errorf("create job: %w", err)
    }
    return nil
}

func (s *Store) UpdateStatus(ctx context.Context, jobID, status string) error {
    query := `
        UPDATE jobs
        SET status = $1, updated_at = NOW()
        WHERE id = $2
    `
    _, err := s.db.ExecContext(ctx, query, status, jobID)
    if err != nil {
        return fmt.Errorf("update status: %w", err)
    }
    return nil
}

func (s *Store) IncrementAttempts(ctx context.Context, jobID string) error {
    query := `
        UPDATE jobs
        SET attempts = attempts + 1, updated_at = NOW()
        WHERE id = $1
    `
    _, err := s.db.ExecContext(ctx, query, jobID)
    if err != nil {
        return fmt.Errorf("increment attempts: %w", err)
    }
    return nil
}

func (s *Store) AcquireLease(ctx context.Context, jobID, workerID string) error {
    query := `
        UPDATE jobs
        SET status = 'running',
            leased_at = NOW(),
            leased_by = $1,
            updated_at = NOW()
        WHERE id = $2
        AND status = 'pending'
    `
    result, err := s.db.ExecContext(ctx, query, workerID, jobID)
    if err != nil {
        return fmt.Errorf("acquire lease: %w", err)
    }

    rows, err := result.RowsAffected()
    if err != nil {
        return fmt.Errorf("lease rows affected: %w", err)
    }
    if rows == 0 {
        return fmt.Errorf("job %s already leased or not found", jobID)
    }
    return nil
}

func (s *Store) GetStalledJobs(ctx context.Context, olderThan time.Duration) ([]Job, error) {
    query := `
        SELECT id, queue, payload, priority, status, attempts, max_retries, run_at, created_at
        FROM jobs
        WHERE status = 'running'
        AND leased_at < NOW() - $1::interval
    `
    rows, err := s.db.QueryContext(ctx, query, olderThan.String())
    if err != nil {
        return nil, fmt.Errorf("get stalled jobs: %w", err)
    }
    defer rows.Close()

    var jobs []Job
    for rows.Next() {
        var j Job
        err := rows.Scan(
            &j.ID, &j.Queue, &j.Payload, &j.Priority,
            &j.Status, &j.Attempts, &j.MaxRetries,
            &j.RunAt, &j.CreatedAt,
        )
        if err != nil {
            return nil, fmt.Errorf("scanning job: %w", err)
        }
        jobs = append(jobs, j)
    }
    return jobs, nil
}
