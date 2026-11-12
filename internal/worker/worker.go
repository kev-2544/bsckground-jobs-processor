
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"background-job-processor/internal/job"
)

// WorkerPool manages multiple workers processing jobs concurrently.
type WorkerPool struct {
	workerCount int
	jobTimeout  time.Duration

	jobs    <-chan job.Job
	results chan<- job.Result
	tracker *job.Tracker

	wg sync.WaitGroup
}

// NewWorkerPool creates a worker pool with a configurable job timeout.
func NewWorkerPool(
	workerCount int,
	jobTimeout time.Duration,
	jobs <-chan job.Job,
	results chan<- job.Result,
	tracker *job.Tracker,
) *WorkerPool {
	if workerCount <= 0 {
		panic("worker count must be greater than zero")
	}

	if jobTimeout <= 0 {
		panic("job timeout must be greater than zero")
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobTimeout:  jobTimeout,
		jobs:        jobs,
		results:     results,
		tracker:     tracker,
	}
}

// Start launches all workers as independent goroutines.
func (p *WorkerPool) Start() {
	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)

		go p.runWorker(i)
	}
}

// runWorker receives jobs until the jobs channel is closed and drained.
func (p *WorkerPool) runWorker(workerID int) {
	defer p.wg.Done()

	fmt.Printf("Worker %d started\n", workerID)

	for j := range p.jobs {
		fmt.Printf("Worker %d picked up job %d\n", workerID, j.ID)

		if err := p.tracker.UpdateStatus(j.ID, job.StatusProcessing); err != nil {
			fmt.Printf("Tracking error for job %d: %v\n", j.ID, err)
			continue
		}

		// Every job gets its own timeout.
		ctx, cancel := context.WithTimeout(
			context.Background(),
			p.jobTimeout,
		)

		result := processJob(ctx, j, workerID)

		// Release context resources immediately after processing.
		cancel()

		// Record the final state of the job.
		var processingError error

		if result.Error != "" {
			processingError = fmt.Errorf("%s", result.Error)
		}

		if err := p.tracker.Complete(
			j.ID,
			result.Status,
			processingError,
		); err != nil {
			fmt.Printf("Tracking error for job %d: %v\n", j.ID, err)
		}

		// Publish the result for the collector.
		p.results <- result
	}

	fmt.Printf("Worker %d stopped\n", workerID)
}

// processJob simulates a background operation that respects cancellation.
func processJob(
	ctx context.Context,
	j job.Job,
	workerID int,
) job.Result {
	start := time.Now()

	fmt.Printf("Worker %d processing job %d...\n", workerID, j.ID)

	// Different payloads simulate different processing scenarios.
	processingDuration := 1 * time.Second

	switch j.Payload {
	case "fail":
		processingDuration = 1 * time.Second
	case "slow":
		processingDuration = 5 * time.Second
	}

	// Timer represents the time required to process this job.
	timer := time.NewTimer(processingDuration)
	defer timer.Stop()

	result := job.Result{
		JobID: j.ID,
	}

	select {
	case <-timer.C:
		// The simulated processing completed before the timeout.

		if j.Payload == "fail" {
			result.Status = job.StatusFailed
			result.Error = "simulated processing failure"
		} else {
			result.Status = job.StatusCompleted
		}

	case <-ctx.Done():
		// The context expired before processing finished.
		result.Status = job.StatusFailed
		result.Error = fmt.Sprintf(
			"job processing interrupted: %v",
			ctx.Err(),
		)
	}

	result.CompletedAt = time.Now()
	result.ProcessingTime = time.Since(start)

	fmt.Printf(
		"Worker %d finished job %d with status %s\n",
		workerID,
		j.ID,
		result.Status,
	)

	return result
}

// Wait blocks until every worker has exited.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
