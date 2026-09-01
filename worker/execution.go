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

	// Marshal args to JSON array or value (caller may supply a single value/array/object)
	var body []byte
	var err error

	// If the single arg is explicitly provided as a single slice or map and the user intended that,
	// we still marshal the args slice so Submit(Args([]string{...})) becomes JSON array-of-array,
	// which is consistent with previous behavior. Most callers call Args(...) with multiple values.
	body, err = json.Marshal(e.args)
	if err != nil {
		return fmt.Errorf("marshal args: %w", err)
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
