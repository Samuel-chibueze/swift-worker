package worker

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/Samuel-chibueze/swift-worker/queue/memory"
    "github.com/Samuel-chibueze/swift-worker/queue/rabbitmq"
    "github.com/Samuel-chibueze/swift-worker/scheduler"
    "github.com/Samuel-chibueze/swift-worker/types"
)

type App struct {
    mu                 sync.RWMutex
    ctx                context.Context
    cancel             context.CancelFunc
    workers            map[string]*Worker
    Backend            Backend
    backendURL         string
    scheduler          *scheduler.Scheduler
    started            bool
    stopped            bool
    wg                 sync.WaitGroup
    shutdownTimeout    time.Duration
    jobsCh             chan types.Job
    defaultTimeout     time.Duration
    defaultRetries     int
    defaultConcurrency int
}

func New(ctx context.Context, opts ...Option) *App {
    ctx, cancel := context.WithCancel(ctx)

    app := &App{
        ctx:                ctx,
        cancel:             cancel,
        workers:            make(map[string]*Worker),
        jobsCh:             make(chan types.Job, 100),
        shutdownTimeout:    30 * time.Second,
        Backend:            memory.New(ctx),
        defaultTimeout:     5 * time.Minute,
        defaultRetries:     3,
        defaultConcurrency: 1,
    }

    for _, opt := range opts {
        opt(app)
    }

    return app
}

func (a *App) Worker(name string, handler interface{}, opts ...WorkerOption) *Worker {
    a.mu.Lock()
    defer a.mu.Unlock()

    h := WrapHandler(handler)

    w := newWorker(name, h, a)
    w.applyOptions(opts...)
    a.workers[name] = w
    return w
}

func (a *App) Exec(w *Worker) *Execution {
    return &Execution{
        app:    a,
        worker: w,
    }
}

func (a *App) Queue(name string) *Execution {
    return &Execution{
        app:  a,
        name: name,
    }
}

func (a *App) Schedule(expression string, fn func()) *scheduler.Task {
    if a.scheduler == nil {
        a.scheduler = scheduler.New(a.ctx)
    }
    return a.scheduler.Schedule(expression, fn)
}

func (a *App) Start() error {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.started {
        return ErrAlreadyStarted
    }

    if a.backendURL != "" {
        backend, err := rabbitmq.New(a.ctx, a.backendURL)
        if err != nil {
            return fmt.Errorf("init rabbitmq: %w", err)
        }
        a.Backend = backend
    }

    if a.Backend == nil {
        return ErrNoBackend
    }

    if err := a.Backend.Start(a.ctx, a.jobsCh); err != nil {
        return fmt.Errorf("start backend: %w", err)
    }

    a.wg.Add(1)
    go a.poolLoop()

    if a.scheduler != nil {
        a.scheduler.Start()
    }

    a.started = true
    return nil
}

func (a *App) Run() error {
    if err := a.Start(); err != nil {
        return err
    }

    <-a.ctx.Done()

    shutdownCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
    defer cancel()

    return a.Shutdown(shutdownCtx)
}

func (a *App) Shutdown(ctx context.Context) error {
    a.mu.Lock()
    if a.stopped {
        a.mu.Unlock()
        return ErrAlreadyStopped
    }
    a.stopped = true
    a.mu.Unlock()

    a.cancel()

    if a.scheduler != nil {
        a.scheduler.Stop()
    }

    if a.Backend != nil {
        if err := a.Backend.Close(); err != nil {
            return fmt.Errorf("close backend: %w", err)
        }
    }

    done := make(chan struct{})
    go func() {
        a.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (a *App) Close() error {
    a.mu.Lock()
    if a.stopped {
        a.mu.Unlock()
        return ErrAlreadyStopped
    }
    a.stopped = true
    a.mu.Unlock()

    a.cancel()

    if a.Backend != nil {
        return a.Backend.Close()
    }

    return nil
}

func (a *App) poolLoop() {
    defer a.wg.Done()

    active := make(map[string]int)
    var mu sync.Mutex

    for {
        select {
        case <-a.ctx.Done():
            return

        case job, ok := <-a.jobsCh:
            if !ok {
                return
            }

            a.mu.RLock()
            worker, exists := a.workers[job.Worker]
            a.mu.RUnlock()

            if !exists {
                continue
            }

            worker.mu.RLock()
            concurrency := worker.Concurrency
            timeout := worker.Timeout
            handler := worker.Handler
            worker.mu.RUnlock()

            mu.Lock()
            current := active[job.Worker]
            if current >= concurrency {
                mu.Unlock()
                a.jobsCh <- job
                continue
            }
            active[job.Worker] = current + 1
            mu.Unlock()

            a.wg.Add(1)
            go func() {
                defer func() {
                    mu.Lock()
                    active[job.Worker]--
                    mu.Unlock()
                    a.wg.Done()
                }()

                var execCtx context.Context
                var cancel context.CancelFunc
                if timeout > 0 {
                    execCtx, cancel = context.WithTimeout(a.ctx, timeout)
                    defer cancel()
                } else {
                    execCtx = a.ctx
                }

                defer func() {
                    if r := recover(); r != nil {
                        // Panic recovered
                    }
                }()

                // Args is already any - pass directly!
                var args []any
                if job.Args == nil {
                    // No args
                    _ = handler(execCtx)
                    return
                }

                // If args is already a slice, use it
                switch v := job.Args.(type) {
                case []any:
                    args = v
                default:
                    // Single argument or other type - wrap it
                    args = []any{job.Args}
                }

                fmt.Printf("[PoolLoop] ?? Calling handler with args: %+v\n", args)
                _ = handler(execCtx, args...)
            }()
        }
    }
}
