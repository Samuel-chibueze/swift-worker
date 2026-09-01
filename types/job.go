package types

import (
	"encoding/json"
	"time"
)

// Job is the shared type used across all packages
type Job struct {
	ID        string          `json:"id"`
	Worker    string          `json:"worker"`
	Args      json.RawMessage `json:"args"`
	CreatedAt time.Time       `json:"created_at"`
	Attempts  int             `json:"attempts,omitempty"`
}

// BackendJob is the internal representation sent from Backends to the worker pool.
// Ack and Nack functions are provided by the backend (e.g. RabbitMQ delivery ack/nack).
// These functions are not serialized and should be treated as runtime-only.
type BackendJob struct {
	Job  Job
	Ack  func() error             `json:"-"`
	Nack func(requeue bool) error `json:"-"`
}
