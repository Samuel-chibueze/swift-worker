package worker

import (
	"encoding/json"
	"time"
)

type Job struct {
	ID        string          `json:"id"`
	Worker    string          `json:"worker"`
	Args      json.RawMessage `json:"args"`
	CreatedAt time.Time       `json:"created_at"`
}
