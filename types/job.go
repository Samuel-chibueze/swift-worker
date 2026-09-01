package types

import (
    "time"
)

// Job is the shared type used across all packages
// Args is ANY type - no marshaling!
type Job struct {
    ID        string    `json:"id"`
    Worker    string    `json:"worker"`
    Args      any       `json:"args"`  // ANY type - no marshaling!
    CreatedAt time.Time `json:"created_at"`
}
