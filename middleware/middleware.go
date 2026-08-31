package middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

type Middleware func(next worker.Handler) worker.Handler

func Chain(middleware ...Middleware) Middleware {
	return func(next worker.Handler) worker.Handler {
		for i := len(middleware) - 1; i >= 0; i-- {
			next = middleware[i](next)
		}
		return next
	}
}

func Logging(logger func(msg string, args ...any)) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, job worker.Job) error {
			start := time.Now()
			err := next(ctx, job)
			duration := time.Since(start)

			if logger != nil {
				if err != nil {
					logger("job failed", "id", job.ID, "worker", job.Worker, "duration", duration, "error", err)
				} else {
					logger("job completed", "id", job.ID, "worker", job.Worker, "duration", duration)
				}
			}

			return err
		}
	}
}

func Recovery() Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, job worker.Job) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, job)
		}
	}
}
