package types

import (
    "time"
)

// Job is the shared type
// Args is raw JSON bytes - NO unmarshaling!
type Job struct {
    ID        string    `json:"id"`
    Worker    string    `json:"worker"`
    Args      []byte    `json:"args"`  // Raw JSON bytes
    CreatedAt time.Time `json:"created_at"`
}
