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

	for {
		select {
		case <-a.ctx.Done():
			return

		case bjob, ok := <-a.jobsCh:
			if !ok {
				return
			}

			job := bjob.Job
			// Find the registered worker
			a.mu.RLock()
			worker, exists := a.workers[job.Worker]
			a.mu.RUnlock()

			if !exists {
				// Unknown worker: do not requeue indefinitely. Reject if possible.
				if bjob.Nack != nil {
					_ = bjob.Nack(false)
				} else {
					// Try to ack if ack exists
					if bjob.Ack != nil {
						_ = bjob.Ack()
					}
				}
				fmt.Printf("[Pool] Unknown worker: %s (job %s rejected)\n", job.Worker, job.ID)
				continue
			}

			// Attempt to acquire worker concurrency slot (non-blocking)
			acquired := false
			select {
			case worker.sem <- struct{}{}:
				acquired = true
			default:
				acquired = false
			}

			if !acquired {
				// Worker is at capacity. Try to re-enqueue the job so it can be retried later.
				if err := a.Backend.Enqueue(a.ctx, job); err != nil {
					// If we can't re-enqueue, try to Nack with requeue=true if possible
					if bjob.Nack != nil {
						_ = bjob.Nack(true)
					} else {
						// As a last resort, log and continue (job may be dropped depending on backend)
						fmt.Printf("[Pool] Could not re-enqueue job %s: %v\n", job.ID, err)
					}
				} else {
					// Successfully re-enqueued: acknowledge original if backend provided ack (e.g. RabbitMQ)
					if bjob.Ack != nil {
						_ = bjob.Ack()
					}
				}
				continue
			}

			// We have a slot reserved; execute in goroutine.
			a.wg.Add(1)
			go func(bj types.BackendJob, w *Worker) {
				defer func() {
					// Release slot
					select {
					case <-w.sem:
					default:
					}
					a.wg.Done()
				}()

				// Prepare execution context with timeout if configured
				var execCtx context.Context
				var cancel context.CancelFunc
				w.mu.RLock()
				timeout := w.Timeout
				maxRetries := w.MaxRetries
				handler := w.Handler
				w.mu.RUnlock()

				if timeout > 0 {
					execCtx, cancel = context.WithTimeout(a.ctx, timeout)
					defer cancel()
				} else {
					execCtx = a.ctx
				}

				// Convert JSON args into []any
				var args []any
				raw := bj.Job.Args

				if len(raw) == 0 || string(raw) == "null" {
					args = []any{}
				} else {
					first := raw[0]
					if first == '[' {
						var arr []any
						if err := json.Unmarshal(raw, &arr); err != nil {
							// Fallback: try single value
							var v any
							_ = json.Unmarshal(raw, &v)
							args = []any{v}
						} else {
							args = arr
						}
					} else {
						var v any
						if err := json.Unmarshal(raw, &v); err != nil {
							args = []any{}
						} else {
							args = []any{v}
						}
					}
				}

				// Execute handler
				start := time.Now()
				err := handler(execCtx, args...)
				duration := time.Since(start)

				if err != nil {
					// Execution failed: apply retry policy
					if bj.Job.Attempts < maxRetries {
						newJob := bj.Job
						newJob.Attempts = newJob.Attempts + 1
						if enqueueErr := a.Backend.Enqueue(a.ctx, newJob); enqueueErr != nil {
							// Could not re-enqueue: Nack with requeue if possible
							if bj.Nack != nil {
								_ = bj.Nack(true)
							} else {
								fmt.Printf("[Pool] Failed to re-enqueue job %s: %v\n", newJob.ID, enqueueErr)
							}
						} else {
							// Re-enqueued successfully: Ack original if possible
							if bj.Ack != nil {
								_ = bj.Ack()
							}
						}
					} else {
						// Exceeded retries: reject permanently
						if bj.Nack != nil {
							_ = bj.Nack(false)
						} else if bj.Ack != nil {
							_ = bj.Ack()
						}
						fmt.Printf("[Pool] Job %s permanently failed after %d attempts: %v\n", bj.Job.ID, bj.Job.Attempts, err)
					}
					return
				}

				// Success: Ack if available
				if bj.Ack != nil {
					_ = bj.Ack()
				}
				_ = duration
			}(bjob, worker)
		}
	}
}
