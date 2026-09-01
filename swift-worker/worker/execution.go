package worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Samuel-chibueze/swift-worker/types"
	"github.com/google/uuid"
)

type Execution struct {
	app      *App
	worker   *Worker
	name     string
	args     []any
	isClient bool
}

func (e *Execution) Args(args ...any) *Execution {
	e.args = args
	return e
}

func (e *Execution) Submit() error {
	if e.app == nil {
		return fmt.Errorf("app is nil")
	}

	name := e.name
	if e.worker != nil {
		name = e.worker.Name
	}

	if name == "" {
		return fmt.Errorf("worker name is empty")
	}

	// Marshal args to JSON exactly once.
	var body []byte
	var err error

	// Ensure zero-argument submissions produce a deterministic JSON representation (empty array).
	if len(e.args) == 0 {
		body = []byte("[]")
	} else {
		body, err = json.Marshal(e.args)
		if err != nil {
			return fmt.Errorf("marshal args: %w", err)
		}
	}

	job := types.Job{
		ID:        uuid.New().String(),
		Worker:    name,
		Args:      json.RawMessage(body),
		CreatedAt: time.Now().UTC(),
		Attempts:  0,
	}

	if e.app.Backend == nil {
		return ErrNoBackend
	}

	return e.app.Backend.Enqueue(e.app.ctx, job)
}
