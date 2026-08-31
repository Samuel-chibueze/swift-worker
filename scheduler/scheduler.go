package scheduler

import (
    "context"
    "sync"

    "github.com/robfig/cron/v3"
)

type Scheduler struct {
    mu      sync.RWMutex
    ctx     context.Context
    cancel  context.CancelFunc
    cron    *cron.Cron
    tasks   []*Task
    running bool
    wg      sync.WaitGroup
}

func New(ctx context.Context) *Scheduler {
    ctx, cancel := context.WithCancel(ctx)

    return &Scheduler{
        ctx:    ctx,
        cancel: cancel,
        cron:   cron.New(cron.WithSeconds()),
        tasks:  make([]*Task, 0),
    }
}

func (s *Scheduler) Schedule(expression string, fn func()) *Task {
    s.mu.Lock()
    defer s.mu.Unlock()

    task := &Task{
        Expression: expression,
        Handler:    fn,
        scheduler:  s,
    }

    s.tasks = append(s.tasks, task)

    if s.running {
        task.start()
    }

    return task
}

func (s *Scheduler) Start() {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.running {
        return
    }

    s.running = true

    for _, task := range s.tasks {
        task.start()
    }

    s.cron.Start()

    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        <-s.ctx.Done()
        s.cron.Stop()
    }()
}

func (s *Scheduler) Stop() {
    s.mu.Lock()
    if !s.running {
        s.mu.Unlock()
        return
    }
    s.running = false
    s.mu.Unlock()

    s.cancel()
    s.wg.Wait()
}
