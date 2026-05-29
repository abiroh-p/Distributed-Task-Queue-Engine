package events

import "time"

type Event struct {
    Type      string    `json:"type"`
    JobID     string    `json:"job_id"`
    Status    string    `json:"status"`
    WorkerID  string    `json:"worker_id,omitempty"`
    Timestamp time.Time `json:"timestamp"`
}