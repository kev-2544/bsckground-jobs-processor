
package worker

import (
	"fmt"
	"sync"
	"time"

	"background-job-processor/internal/job"
)

// WorkerPool manages multiple workers that process jobs concurrently.
type WorkerPool struct {
	workerCount int
	jobs        <-chan job.Job
	wg          sync.WaitGroup
}

// NewWorkerPool initializes a worker pool.
func NewWorkerPool(workerCount int, jobs <-chan job.Job) *WorkerPool {
	if workerCount <= 0 {
		panic("worker count must be greater than zero")
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobs:        jobs,
	}
}

// Start launches all workers as separate goroutines.
func (p *WorkerPool) Start() {
	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)

		// Each goroutine runs the worker function independently.
		go p.runWorker(i)
	}
}

// runWorker continuously receives and processes jobs.
func (p *WorkerPool) runWorker(workerID int) {
	defer p.wg.Done()

	fmt.Printf("Worker %d started\n", workerID)

	// A range over a channel receives values until the channel is closed
	// and all buffered values have been consumed.
	for j := range p.jobs {
		fmt.Printf(
			"Worker %d picked up job %d: %s\n",
			workerID,
			j.ID,
			j.Name,
		)

		// Simulate actual background processing.
		processJob(j, workerID)
	}

	fmt.Printf("Worker %d stopped\n", workerID)
}

// processJob simulates performing the actual job.
func processJob(j job.Job, workerID int) {
	fmt.Printf("Worker %d processing job %d...\n", workerID, j.ID)

	// Simulate different processing durations.
	time.Sleep(2 * time.Second)

	fmt.Printf("Worker %d completed job %d\n", workerID, j.ID)
}

// Wait blocks until all workers have finished.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
