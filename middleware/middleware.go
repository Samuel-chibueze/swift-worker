package middleware

import (
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

// Logging middleware - wraps handler with logging
func Logging(logger func(msg string, args ...any)) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(args ...any) error {
			start := time.Now()

			// Extract job info if available
			var jobID, workerName string
			if len(args) > 0 {
				// Try to get job info from args
				if job, ok := args[0].(interface{ GetID() string }); ok {
					jobID = job.GetID()
				}
			}

			err := next(args...)
			duration := time.Since(start)

			if logger != nil {
				if err != nil {
					logger("job failed",
						"job_id", jobID,
						"worker", workerName,
						"duration", duration,
						"error", err)
				} else {
					logger("job completed",
						"job_id", jobID,
						"worker", workerName,
						"duration", duration)
				}
			}

			return err
		}
	}
}

// Recovery middleware - recovers from panics
func Recovery() Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(args ...any) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(args...)
		}
	}
}

// Timeout middleware - adds timeout to handler execution
func Timeout(timeout time.Duration) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(args ...any) error {
			done := make(chan error, 1)
			go func() {
				done <- next(args...)
			}()

			select {
			case err := <-done:
				return err
			case <-time.After(timeout):
				return fmt.Errorf("timeout after %v", timeout)
			}
		}
	}
}
