package worker

import (
    "sync"
    "time"
)

type Worker struct {
    mu          sync.RWMutex
    Name        string
    Handler     Handler
    Concurrency int
    Timeout     time.Duration
    MaxRetries  int
    app         *App
}

// Defaults: concurrency=1, timeout=30s, maxRetries=3
func newWorker(name string, handler Handler, app *App) *Worker {
    return &Worker{
        Name:        name,
        Handler:     handler,
        Concurrency: 1,
        Timeout:     30 * time.Second,
        MaxRetries:  3,
        app:         app,
    }
}

func (w *Worker) applyOptions(opts ...WorkerOption) {
    w.mu.Lock()
    defer w.mu.Unlock()
    for _, opt := range opts {
        opt(w)
    }
}
