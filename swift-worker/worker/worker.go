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
	sem         chan struct{}
}

func newWorker(name string, handler Handler, app *App) *Worker {
	w := &Worker{
		Name:        name,
		Handler:     handler,
		Concurrency: app.defaultConcurrency,
		Timeout:     app.defaultTimeout,
		MaxRetries:  app.defaultRetries,
		app:         app,
	}
	// initialize semaphore with the configured concurrency
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	w.sem = make(chan struct{}, w.Concurrency)
	return w
}

func (w *Worker) applyOptions(opts ...WorkerOption) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, opt := range opts {
		opt(w)
	}
	// Recreate semaphore to reflect new concurrency
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	// Replace sem with a new buffered channel of the new capacity.
	w.sem = make(chan struct{}, w.Concurrency)
}
