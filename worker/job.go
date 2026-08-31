package worker

import (
    "time"
)

// Job is INTERNAL - users never see this.
type Job struct {
    ID        string    `json:"id"`
    Worker    string    `json:"worker"`
    Args      []byte    `json:"args"`
    CreatedAt time.Time `json:"created_at"`
}
