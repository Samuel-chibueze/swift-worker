package worker

import (
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

    // NO marshaling - pass args as-is!
    job := types.Job{
        ID:        uuid.New().String(),
        Worker:    name,
        Args:      e.args,  // Pass through raw
        CreatedAt: time.Now().UTC(),
    }

    if e.app.Backend == nil {
        return ErrNoBackend
    }

    return e.app.Backend.Enqueue(e.app.ctx, job)
}
