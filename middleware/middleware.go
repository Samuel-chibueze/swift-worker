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

// Logging middleware - logs job execution with context
func Logging(logger func(msg string, args ...any)) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, args ...any) error {
			start := time.Now()
			err := next(ctx, args...)
			duration := time.Since(start)

			if logger != nil {
				if err != nil {
					logger("job failed",
						"duration", duration,
						"error", err,
						"args", args)
				} else {
					logger("job completed",
						"duration", duration,
						"args", args)
				}
			}

			return err
		}
	}
}

// Recovery middleware - recovers from panics
func Recovery() Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, args ...any) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, args...)
		}
	}
}

// Timeout middleware - adds timeout to job execution
func Timeout(timeout time.Duration) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, args ...any) error {
			done := make(chan error, 1)
			go func() {
				done <- next(ctx, args...)
			}()

			select {
			case err := <-done:
				return err
			case <-time.After(timeout):
				return fmt.Errorf("timeout after %v", timeout)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// ContextLogger - logs context information
func ContextLogger(logger func(msg string, args ...any)) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, args ...any) error {
			// Log context info
			if logger != nil {
				// Check if context has deadline
				deadline, hasDeadline := ctx.Deadline()
				if hasDeadline {
					logger("context has deadline",
						"deadline", deadline,
						"time_left", time.Until(deadline))
				}
			}
			return next(ctx, args...)
		}
	}
}

// WithContext - adds values to context for downstream handlers
func WithContext(key, value interface{}) Middleware {
	return func(next worker.Handler) worker.Handler {
		return func(ctx context.Context, args ...any) error {
			ctx = context.WithValue(ctx, key, value)
			return next(ctx, args...)
		}
	}
}
