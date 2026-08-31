package scheduler

import (
	"sync"

	"github.com/robfig/cron/v3"
)

type Task struct {
	mu         sync.RWMutex
	Expression string
	Handler    func()
	scheduler  *Scheduler
	entryID    cron.EntryID
	running    bool
}

func (t *Task) start() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.running || t.scheduler == nil || t.scheduler.cron == nil {
		return
	}

	id, err := t.scheduler.cron.AddFunc(t.Expression, t.Handler)
	if err != nil {
		return
	}

	t.entryID = id
	t.running = true
}

func (t *Task) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.running {
		return
	}

	if t.scheduler != nil && t.scheduler.cron != nil {
		t.scheduler.cron.Remove(t.entryID)
	}

	t.running = false
}
