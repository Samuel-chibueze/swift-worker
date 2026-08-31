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

func Logging(logger func(msg string, args ...any)) Middleware {
    return func(next worker.Handler) worker.Handler {
        return func(args ...any) error {
            start := time.Now()
            err := next(args...)
            duration := time.Since(start)

            if logger != nil {
                if err != nil {
                    logger("job failed", "duration", duration, "error", err, "args", args)
                } else {
                    logger("job completed", "duration", duration, "args", args)
                }
            }

            return err
        }
    }
}

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
