package queue

import (
	"fmt"
	"sync"

	"background-job-processor/internal/job"
)

// Queue manages jobs using a buffered channel.
type Queue struct {
	jobs chan job.Job

	// Protects submission and channel closure.
	mu     sync.RWMutex
	closed bool
}

// NewQueue creates a buffered job queue.
func NewQueue(capacity int) *Queue {
	if capacity <= 0 {
		panic("queue capacity must be greater than zero")
	}

	return &Queue{
		jobs: make(chan job.Job, capacity),
	}
}

// Submit adds a job without blocking when the queue is full.
func (q *Queue) Submit(j job.Job) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return fmt.Errorf("job queue is closed")
	}

	select {
	case q.jobs <- j:
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

// Jobs exposes a receive-only channel to workers.
func (q *Queue) Jobs() <-chan job.Job {
	return q.jobs
}

// Close prevents further submissions and closes the jobs channel.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
}

// Length returns the number of jobs waiting in the buffer.
func (q *Queue) Length() int {
	return len(q.jobs)
}
