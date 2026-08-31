package memory

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"
    "time"
)

// Job is defined locally - no import from worker
type Job struct {
    ID        string          `json:"id"`
    Worker    string          `json:"worker"`
    Args      json.RawMessage `json:"args"`
    CreatedAt time.Time       `json:"created_at"`
}

type Backend struct {
    mu     sync.Mutex
    queue  []Job
    cond   *sync.Cond
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
    closed bool
}

func New(ctx context.Context) *Backend {
    ctx, cancel := context.WithCancel(ctx)
    b := &Backend{
        queue:  make([]Job, 0),
        ctx:    ctx,
        cancel: cancel,
    }
    b.cond = sync.NewCond(&b.mu)
    return b
}

// Enqueue accepts interface{} and converts to local Job
func (b *Backend) Enqueue(ctx context.Context, job interface{}) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.closed {
        return fmt.Errorf("queue is closed")
    }

    var j Job
    
    // Convert interface{} to Job
    switch v := job.(type) {
    case map[string]interface{}:
        // Try to convert from map
        data, err := json.Marshal(v)
        if err != nil {
            return fmt.Errorf("marshal job: %w", err)
        }
        if err := json.Unmarshal(data, &j); err != nil {
            return fmt.Errorf("unmarshal job: %w", err)
        }
    case Job:
        j = v
    case *Job:
        j = *v
    default:
        // Try JSON marshaling
        data, err := json.Marshal(job)
        if err != nil {
            return fmt.Errorf("marshal job: %w", err)
        }
        if err := json.Unmarshal(data, &j); err != nil {
            return fmt.Errorf("unmarshal job: %w", err)
        }
    }

    b.queue = append(b.queue, j)
    b.cond.Signal()
    return nil
}

func (b *Backend) Start(ctx context.Context, jobs chan<- interface{}) error {
    b.wg.Add(1)
    go b.consumeLoop(ctx, jobs)
    return nil
}

func (b *Backend) consumeLoop(ctx context.Context, jobs chan<- interface{}) {
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
