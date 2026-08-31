package worker

import (
    "context"
    "github.com/Samuel-chibueze/swift-worker/types"
)

type Backend interface {
    Enqueue(ctx context.Context, job types.Job) error
    Start(ctx context.Context, jobs chan<- types.Job) error
    Close() error
}
