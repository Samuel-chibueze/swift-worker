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

func newWorker(name string, handler Handler, app *App) *Worker {
    return &Worker{
        Name:        name,
        Handler:     handler,
        Concurrency: app.defaultConcurrency,
        Timeout:     app.defaultTimeout,
        MaxRetries:  app.defaultRetries,
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
