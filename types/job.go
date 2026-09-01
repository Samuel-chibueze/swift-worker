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
}
