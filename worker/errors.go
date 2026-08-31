package worker

import "errors"

var (
	ErrWorkerNotFound     = errors.New("worker not found")
	ErrAlreadyStarted     = errors.New("app already started")
	ErrAlreadyStopped     = errors.New("app already stopped")
	ErrNoBackend          = errors.New("no backend configured")
	ErrInvalidConcurrency = errors.New("concurrency must be > 0")
	ErrInvalidTimeout     = errors.New("timeout must be > 0")
)
