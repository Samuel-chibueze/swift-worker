package worker

import "context"

type Backend interface {
    Enqueue(ctx context.Context, job Job) error
    Start(ctx context.Context, jobs chan<- Job) error
    Close() error
}
