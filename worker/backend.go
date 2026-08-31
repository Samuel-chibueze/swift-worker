package worker

import "context"

// Backend interface uses interface{} to avoid type conflicts
type Backend interface {
	Enqueue(ctx context.Context, job interface{}) error
	Start(ctx context.Context, jobs chan<- interface{}) error
	Close() error
}
