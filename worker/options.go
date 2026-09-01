package worker

import "time"

// ============================================================
// APP-LEVEL OPTIONS (GLOBAL DEFAULTS)
// ============================================================

type Option func(*App)

func WithDefaultTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.defaultTimeout = d
		}
	}
}

func WithDefaultRetries(n int) Option {
	return func(a *App) {
		if n >= 0 {
			a.defaultRetries = n
		}
	}
}

func WithDefaultConcurrency(n int) Option {
	return func(a *App) {
		if n > 0 {
			a.defaultConcurrency = n
		}
	}
}

func WithRabbitMQ(url string) Option {
	return func(a *App) {
		a.backendURL = url
	}
}

func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}

// ============================================================
// WORKER-LEVEL OPTIONS (PER-WORKER OVERRIDES)
// ============================================================

type WorkerOption func(*Worker)

func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		if n > 0 {
			w.Concurrency = n
		}
	}
}

func WithTimeout(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d >= 0 {
			w.Timeout = d
		}
	}
}

func WithMaxRetries(n int) WorkerOption {
	return func(w *Worker) {
		if n >= 0 {
			w.MaxRetries = n
		}
	}
}
