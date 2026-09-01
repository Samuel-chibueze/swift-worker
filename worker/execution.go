package worker

import (
    "encoding/json"
    "fmt"
    "time"

    "github.com/google/uuid"
    "github.com/Samuel-chibueze/swift-worker/types"
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

    // Marshal args to JSON ONCE here
    argsJSON, err := json.Marshal(e.args)
    if err != nil {
        return fmt.Errorf("marshal args: %w", err)
    }

    job := types.Job{
        ID:        uuid.New().String(),
        Worker:    name,
        Args:      argsJSON,  // Raw JSON bytes
        CreatedAt: time.Now().UTC(),
    }

    if e.app.Backend == nil {
        return ErrNoBackend
    }

    return e.app.Backend.Enqueue(e.app.ctx, job)
}
