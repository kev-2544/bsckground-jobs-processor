
package queue

import (
	"fmt"

	"background-job-processor/internal/job"
)

// Queue manages jobs using a buffered channel.
type Queue struct {
	jobs chan job.Job
}

// NewQueue creates a queue with a specified capacity.
func NewQueue(capacity int) *Queue {
	if capacity <= 0 {
		panic("queue capacity must be greater than zero")
	}

	return &Queue{
		// The second argument creates a buffered channel.
		// It allows jobs to wait in the queue before workers receive them.
		jobs: make(chan job.Job, capacity),
	}
}

// Submit places a job into the queue.
func (q *Queue) Submit(j job.Job) error {
	select {
	case q.jobs <- j:
		// The job was successfully placed in the channel.
		return nil

	default:
		// The channel buffer is full.
		return fmt.Errorf("job queue is full")
	}
}

// Jobs exposes the receive-only channel to workers.
func (q *Queue) Jobs() <-chan job.Job {
	return q.jobs
}

// Close signals that no more jobs will be submitted.
func (q *Queue) Close() {
	close(q.jobs)
}

// Length returns the number of jobs currently waiting in the buffer.
func (q *Queue) Length() int {
	return len(q.jobs)
}
