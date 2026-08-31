package worker

import (
	"time"
)

type Option func(*App)
type WorkerOption func(*Worker)

func WithRabbitMQ(url string) Option {
	return func(a *App) {
		a.backendURL = url
	}
}

func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		if n <= 0 {
			n = 1
		}
		w.Concurrency = n
	}
}

func WithTimeout(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d <= 0 {
			d = 30 * time.Second
		}
		w.Timeout = d
	}
}

func WithMaxRetries(n int) WorkerOption {
	return func(w *Worker) {
		if n < 0 {
			n = 0
		}
		w.MaxRetries = n
	}
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d <= 0 {
			d = 30 * time.Second
		}
		a.shutdownTimeout = d
	}
}
