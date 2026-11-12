
package worker

import (
	"fmt"
	"sync"
	"time"

	"background-job-processor/internal/job"
)

// WorkerPool manages multiple workers processing jobs concurrently.
type WorkerPool struct {
	workerCount int
	jobs        <-chan job.Job
	results     chan<- job.Result
	tracker     *job.Tracker

	wg sync.WaitGroup
}

// NewWorkerPool initializes the worker pool.
func NewWorkerPool(
	workerCount int,
	jobs <-chan job.Job,
	results chan<- job.Result,
	tracker *job.Tracker,
) *WorkerPool {

	if workerCount <= 0 {
		panic("worker count must be greater than zero")
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobs:        jobs,
		results:     results,
		tracker:     tracker,
	}
}

// Start launches the configured number of workers.
func (p *WorkerPool) Start() {
	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)

		go p.runWorker(i)
	}
}

// runWorker continuously receives jobs from the shared channel.
func (p *WorkerPool) runWorker(workerID int) {
	defer p.wg.Done()

	fmt.Printf("Worker %d started\n", workerID)

	for j := range p.jobs {
		fmt.Printf("Worker %d picked up job %d\n", workerID, j.ID)

		// Mark the job as actively processing.
		if err := p.tracker.UpdateStatus(j.ID, job.StatusProcessing); err != nil {
			fmt.Printf("Worker %d: tracking error: %v\n", workerID, err)
			continue
		}

		// Process the job and capture its result.
		result := processJob(j, workerID)

		// Record the final job state.
		if err := p.tracker.Complete(j.ID, result.Status, errorFromResult(result)); err != nil {
			fmt.Printf("Worker %d: completion tracking error: %v\n", workerID, err)
		}

		// Send the result to the results channel.
		p.results <- result
	}

	fmt.Printf("Worker %d stopped\n", workerID)
}

// processJob simulates a real background task.
func processJob(j job.Job, workerID int) job.Result {
	start := time.Now()

	fmt.Printf("Worker %d processing job %d...\n", workerID, j.ID)

	// Simulate processing time.
	time.Sleep(1 * time.Second)

	result := job.Result{
		JobID:      j.ID,
		Status:     job.StatusCompleted,
		CompletedAt: time.Now(),
	}

	// Simulate a failed job using a specific payload.
	if j.Payload == "fail" {
		result.Status = job.StatusFailed
		result.Error = "simulated processing failure"
	}

	result.ProcessingTime = time.Since(start)

	if result.Status == job.StatusFailed {
		fmt.Printf("Worker %d failed job %d\n", workerID, j.ID)
	} else {
		fmt.Printf("Worker %d completed job %d\n", workerID, j.ID)
	}

	return result
}

// errorFromResult converts a failed result into an error.
func errorFromResult(result job.Result) error {
	if result.Error == "" {
		return nil
	}

	return fmt.Errorf("%s", result.Error)
}

// Wait blocks until every worker has exited.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
