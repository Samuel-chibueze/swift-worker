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
	jobsCh             chan types.BackendJob
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
		jobsCh:             make(chan types.BackendJob, 100),
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

	// Start worker pool loop in background and return immediately
	a.wg.Add(1)
	go a.poolLoop()

	if a.scheduler != nil {
		a.scheduler.Start()
	}

	a.started = true
	return nil
}

func (a *App) Run() error {
	// Start the system (non-blocking)
	if err := a.Start(); err != nil {
		return err
	}

	// Block until context is cancelled
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

	// Cancel internal context to stop accepting new jobs and signal goroutines
	a.cancel()

	if a.scheduler != nil {
		a.scheduler.Stop()
	}

	// Close backend (this should stop RabbitMQ consumer)
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

		case bjob, ok := <-a.jobsCh:
			if !ok {
				return
			}

			job := bjob.Job
			fmt.Printf("[PoolLoop] ?? Got job: %s (worker: %s)\n", job.ID, job.Worker)

			a.mu.RLock()
			worker, exists := a.workers[job.Worker]
			a.mu.RUnlock()

			if !exists {
				fmt.Printf("[PoolLoop] ? Worker not found: %s\n", job.Worker)
				// If this came from a broker, reject without requeue
				if bjob.Nack != nil {
					_ = bjob.Nack(false)
				}
				continue
			}

			worker.mu.RLock()
			concurrency := worker.Concurrency
			timeout := worker.Timeout
			handler := worker.Handler
			maxRetries := worker.MaxRetries
			worker.mu.RUnlock()

			mu.Lock()
			current := active[job.Worker]
			if current >= concurrency {
				mu.Unlock()
				fmt.Printf("[PoolLoop] ? Concurrency limit reached, requeuing\n")
				// Try to re-enqueue the job so it will be retried later.
				// For brokers this will publish a new message; for memory backend this will push back.
				// We attempt to re-enqueue and then Ack/Nack appropriately for broker/backends.
				if err := a.Backend.Enqueue(a.ctx, job); err != nil {
					// If we can't re-enqueue, try to Nack with requeue=true if possible
					if bjob.Nack != nil {
						_ = bjob.Nack(true)
					}
				} else {
					// We re-enqueued successfully; acknowledge original if we have an ack function
					if bjob.Ack != nil {
						_ = bjob.Ack()
					}
				}
				continue
			}
			active[job.Worker] = current + 1
			mu.Unlock()

			a.wg.Add(1)
			go func(bj types.BackendJob) {
				defer func() {
					mu.Lock()
					active[bj.Job.Worker]--
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

				// Prepare handler arguments from JSON.
				var args []any
				raw := bj.Job.Args

				// Determine JSON kind
				if len(raw) == 0 || string(raw) == "null" {
					args = []any{}
				} else {
					first := raw[0]
					if first == '[' {
						// JSON array -> unmarshal into []any
						var arr []any
						if err := json.Unmarshal(raw, &arr); err != nil {
							fmt.Printf("[PoolLoop] ? Unmarshal array error: %v (Args: %s)\n", err, string(raw))
							// treat as single raw value fallback
							var v any
							_ = json.Unmarshal(raw, &v)
							args = []any{v}
						} else {
							args = arr
						}
					} else {
						// Single JSON value or object -> unmarshal into interface{} and pass as single arg
						var v any
						if err := json.Unmarshal(raw, &v); err != nil {
							fmt.Printf("[PoolLoop] ? Unmarshal single value error: %v (Args: %s)\n", err, string(raw))
							args = []any{}
						} else {
							args = []any{v}
						}
					}
				}

				fmt.Printf("[PoolLoop] ?? Calling handler with: %+v\n", args)
				start := time.Now()
				err := handler(execCtx, args...)
				duration := time.Since(start)

				if err != nil {
					fmt.Printf("[PoolLoop] ? Handler error: %v (duration: %s)\n", err, duration)
					// Retry logic
					if bj.Job.Attempts < maxRetries {
						// Increment attempts and re-enqueue via backend (publish)
						newJob := bj.Job
						newJob.Attempts = newJob.Attempts + 1
						// Preserve CreatedAt as original or set to now? keep original
						if enqueueErr := a.Backend.Enqueue(a.ctx, newJob); enqueueErr != nil {
							fmt.Printf("[PoolLoop] ? Failed to re-enqueue job %s: %v\n", newJob.ID, enqueueErr)
							// Can't re-enqueue - Nack with requeue=true if possible
							if bj.Nack != nil {
								_ = bj.Nack(true)
							}
						} else {
							// Successfully re-enqueued - Ack original to remove it from broker
							if bj.Ack != nil {
								_ = bj.Ack()
							}
						}
					} else {
						// Exceeded retries - reject permanently (Nack without requeue if possible)
						fmt.Printf("[PoolLoop] ? Job %s permanently failed after %d attempts\n", bj.Job.ID, bj.Job.Attempts)
						if bj.Nack != nil {
							_ = bj.Nack(false)
						}
					}
				} else {
					fmt.Printf("[PoolLoop] ? Handler completed (duration: %s)\n", duration)
					// Success - Ack the message if backend provided ack
					if bj.Ack != nil {
						_ = bj.Ack()
					}
				}
			}(bjob)
		}
	}
}
