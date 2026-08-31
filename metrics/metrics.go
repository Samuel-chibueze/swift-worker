package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	jobsSubmitted atomic.Int64
	jobsStarted   atomic.Int64
	jobsCompleted atomic.Int64
	jobsFailed    atomic.Int64
	totalDuration atomic.Int64
}

func New() *Metrics {
	return &Metrics{}
}

func (m *Metrics) JobSubmitted() { m.jobsSubmitted.Add(1) }
func (m *Metrics) JobStarted()   { m.jobsStarted.Add(1) }

func (m *Metrics) JobCompleted(duration time.Duration) {
	m.jobsCompleted.Add(1)
	m.totalDuration.Add(int64(duration))
}

func (m *Metrics) JobFailed() { m.jobsFailed.Add(1) }

func (m *Metrics) Stats() map[string]any {
	completed := m.jobsCompleted.Load()
	avgDuration := int64(0)
	if completed > 0 {
		avgDuration = m.totalDuration.Load() / completed / int64(time.Millisecond)
	}

	return map[string]any{
		"jobs_submitted":  m.jobsSubmitted.Load(),
		"jobs_started":    m.jobsStarted.Load(),
		"jobs_completed":  completed,
		"jobs_failed":     m.jobsFailed.Load(),
		"avg_duration_ms": avgDuration,
	}
}
