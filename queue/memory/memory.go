package memory

import (
    "context"
    "fmt"
    "sync"

    "github.com/Samuel-chibueze/swift-worker/types"
)

type Backend struct {
    mu     sync.Mutex
    queue  []types.Job
    cond   *sync.Cond
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
    closed bool
}

func New(ctx context.Context) *Backend {
    ctx, cancel := context.WithCancel(ctx)
    b := &Backend{
        queue:  make([]types.Job, 0),
        ctx:    ctx,
        cancel: cancel,
    }
    b.cond = sync.NewCond(&b.mu)
    return b
}

func (b *Backend) Enqueue(ctx context.Context, job types.Job) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.closed {
        return fmt.Errorf("queue is closed")
    }

    b.queue = append(b.queue, job)
    b.cond.Signal()
    return nil
}

func (b *Backend) Start(ctx context.Context, jobs chan<- types.Job) error {
    b.wg.Add(1)
    go b.consumeLoop(ctx, jobs)
    return nil
}

func (b *Backend) consumeLoop(ctx context.Context, jobs chan<- types.Job) {
    defer b.wg.Done()

    for {
        b.mu.Lock()
        for len(b.queue) == 0 && !b.closed {
            b.cond.Wait()
        }

        if b.closed {
            b.mu.Unlock()
            return
        }

        job := b.queue[0]
        b.queue = b.queue[1:]
        b.mu.Unlock()

        select {
        case jobs <- job:
        case <-ctx.Done():
            return
        }
    }
}

func (b *Backend) Close() error {
    b.mu.Lock()
    b.closed = true
    b.mu.Unlock()

    b.cancel()
    b.cond.Broadcast()
    b.wg.Wait()
    return nil
}
