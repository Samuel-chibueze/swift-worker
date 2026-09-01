package worker

import (
    "context"
    "encoding/json"
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

    fmt.Println("[App] ? New() created")
    return app
}

func (a *App) Worker(name string, handler interface{}, opts ...WorkerOption) *Worker {
    a.mu.Lock()
    defer a.mu.Unlock()

    h := WrapHandler(handler)

    w := newWorker(name, h, a)
    w.applyOptions(opts...)
    a.workers[name] = w
    fmt.Printf("[App] ? Worker registered: %s\n", name)
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
    fmt.Println("[App] ?? Start() called")

    a.mu.Lock()
    defer a.mu.Unlock()

    if a.started {
        fmt.Println("[App] ?? Already started")
        return ErrAlreadyStarted
    }

    fmt.Printf("[App] ?? backendURL: '%s'\n", a.backendURL)

    if a.backendURL != "" {
        fmt.Printf("[App] ?? Creating RabbitMQ backend for: %s\n", a.backendURL)
        backend, err := rabbitmq.New(a.ctx, a.backendURL)
        if err != nil {
            fmt.Printf("[App] ? RabbitMQ creation failed: %v\n", err)
            return fmt.Errorf("init rabbitmq: %w", err)
        }
        a.Backend = backend
        fmt.Println("[App] ? RabbitMQ backend created")
    }

    if a.Backend == nil {
        fmt.Println("[App] ? No backend configured")
        return ErrNoBackend
    }

    fmt.Printf("[App] ?? Backend type: %T\n", a.Backend)

    fmt.Println("[App] ?? Starting backend consumer...")
    if err := a.Backend.Start(a.ctx, a.jobsCh); err != nil {
        fmt.Printf("[App] ? Backend Start failed: %v\n", err)
        return fmt.Errorf("start backend: %w", err)
    }
    fmt.Println("[App] ? Backend consumer started")

    fmt.Println("[App] ?? Starting pool loop...")
    a.wg.Add(1)
    go a.poolLoop()
    fmt.Println("[App] ? Pool loop started (goroutine launched)")

    if a.scheduler != nil {
        a.scheduler.Start()
    }

    a.started = true
    fmt.Println("[App] ? Start() completed successfully")
    return nil
}

func (a *App) Run() error {
    fmt.Println("[App] ?? Run() called")
    if err := a.Start(); err != nil {
        return err
    }

    fmt.Println("[App] ? Waiting for context cancellation...")
    <-a.ctx.Done()
    fmt.Println("[App] ?? Context cancelled")

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

    fmt.Println("[PoolLoop] ?????? STARTED - waiting for jobs on channel ??????")

    active := make(map[string]int)
    var mu sync.Mutex

    for {
        select {
        case <-a.ctx.Done():
            fmt.Println("[PoolLoop] ? Context cancelled, exiting")
            return

        case job, ok := <-a.jobsCh:
            if !ok {
                fmt.Println("[PoolLoop] ? jobsCh closed, exiting")
                return
            }

            fmt.Printf("[PoolLoop] ?????? GOT JOB: %s for worker: %s ??????\n", job.ID, job.Worker)

            a.mu.RLock()
            worker, exists := a.workers[job.Worker]
            a.mu.RUnlock()

            if !exists {
                fmt.Printf("[PoolLoop] ? Worker not found: %s\n", job.Worker)
                continue
            }

            fmt.Printf("[PoolLoop] ? Found worker: %s\n", worker.Name)

            worker.mu.RLock()
            concurrency := worker.Concurrency
            timeout := worker.Timeout
            handler := worker.Handler
            worker.mu.RUnlock()

            mu.Lock()
            current := active[job.Worker]
            if current >= concurrency {
                mu.Unlock()
                fmt.Printf("[PoolLoop] ? Concurrency limit reached, requeuing\n")
                select {
                case a.jobsCh <- job:
                default:
                }
                continue
            }
            active[job.Worker] = current + 1
            mu.Unlock()

            fmt.Printf("[PoolLoop] ?? Starting goroutine (active: %d/%d)\n", current+1, concurrency)

            a.wg.Add(1)
            go func() {
                defer func() {
                    mu.Lock()
                    active[job.Worker]--
                    mu.Unlock()
                    a.wg.Done()
                    fmt.Printf("[PoolLoop] ? Goroutine finished\n")
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
                        fmt.Printf("[PoolLoop] ?? Panic: %v\n", r)
                    }
                }()

                var args []any
                if err := json.Unmarshal(job.Args, &args); err != nil {
                    fmt.Printf("[PoolLoop] ? Unmarshal error: %v\n", err)
                    _ = handler(execCtx)
                    return
                }

                fmt.Printf("[PoolLoop] ?????? Calling handler with: %+v ??????\n", args)
                err := handler(execCtx, args...)
                if err != nil {
                    fmt.Printf("[PoolLoop] ? Handler error: %v\n", err)
                } else {
                    fmt.Printf("[PoolLoop] ??? Handler completed successfully ???\n")
                }
            }()
        }
    }
}
