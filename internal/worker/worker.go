package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"background-job-processor/internal/job"
)

type WorkerPool struct {
	workerCount int
	jobTimeout  time.Duration
	maxAttempts int
	baseBackoff time.Duration

	jobs        <-chan job.Job
	results     chan<- job.Result
	deadLetters chan<- job.DeadLetter

	tracker *job.Tracker
	wg      sync.WaitGroup
}

func NewWorkerPool(
	workerCount int,
	jobTimeout time.Duration,
	maxAttempts int,
	baseBackoff time.Duration,
	jobs <-chan job.Job,
	results chan<- job.Result,
	deadLetters chan<- job.DeadLetter,
	tracker *job.Tracker,
) *WorkerPool {

	if workerCount <= 0 {
		panic("worker count must be greater than zero")
	}

	if jobTimeout <= 0 {
		panic("job timeout must be greater than zero")
	}

	if maxAttempts <= 0 {
		panic("maximum attempts must be greater than zero")
	}

	if baseBackoff < 0 {
		panic("base backoff cannot be negative")
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobTimeout:  jobTimeout,
		maxAttempts: maxAttempts,
		baseBackoff: baseBackoff,
		jobs:        jobs,
		results:     results,
		deadLetters: deadLetters,
		tracker:     tracker,
	}
}

func (p *WorkerPool) Start() {
	for i := 1; i <= p.workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker(i)
	}
}

func (p *WorkerPool) runWorker(workerID int) {
	defer p.wg.Done()

	fmt.Printf("Worker %d started\n", workerID)

	for j := range p.jobs {
		fmt.Printf("Worker %d picked up job %d\n", workerID, j.ID)

		result := p.processWithRetries(j, workerID)

		// Record the final outcome after all attempts.
		var processingError error
		if result.Error != "" {
			processingError = fmt.Errorf("%s", result.Error)
		}

		finalStatus := result.Status

		if finalStatus == job.StatusFailed {
			finalStatus = job.StatusDeadLetter
			result.Status = finalStatus

			deadLetter := job.DeadLetter{
				Job:         j,
				FinalResult: result,
				FailedAt:    time.Now(),
			}

			// Send the exhausted job to the dead letter channel.
			p.deadLetters <- deadLetter
		}

		if err := p.tracker.Complete(j.ID, finalStatus, processingError); err != nil {
			fmt.Printf("Tracking error for job %d: %v\n", j.ID, err)
		}

		p.results <- result
	}

	fmt.Printf("Worker %d stopped\n", workerID)
}

// processWithRetries controls the complete retry lifecycle of one job.
func (p *WorkerPool) processWithRetries(
	j job.Job,
	workerID int,
) job.Result {

	var result job.Result

	for attempt := 1; attempt <= p.maxAttempts; attempt++ {

		fmt.Printf(
			"Worker %d processing job %d, attempt %d/%d\n",
			workerID,
			j.ID,
			attempt,
			p.maxAttempts,
		)

		if err := p.tracker.UpdateAttempt(
			j.ID,
			attempt,
			job.StatusProcessing,
		); err != nil {
			return job.Result{
				JobID:  j.ID,
				Status: job.StatusFailed,
				Error:  err.Error(),
			}
		}

		// Every attempt receives its own timeout.
		ctx, cancel := context.WithTimeout(
			context.Background(),
			p.jobTimeout,
		)

		result = processJob(ctx, j, workerID, attempt)
		cancel()

		result.JobID = j.ID

		if result.Status == job.StatusCompleted {
			return result
		}

		// No more attempts remain.
		if attempt == p.maxAttempts {
			return result
		}

		// Mark the job as retrying before waiting.
		if err := p.tracker.UpdateAttempt(
			j.ID,
			attempt,
			job.StatusRetrying,
		); err != nil {
			result.Status = job.StatusFailed
			result.Error = err.Error()
			return result
		}

		delay := p.backoffDuration(attempt)

		fmt.Printf(
			"Job %d failed. Retrying in %v\n",
			j.ID,
			delay,
		)

		// The worker waits before making another attempt.
		time.Sleep(delay)
	}

	return result
}

// backoffDuration calculates exponential retry delays.
func (p *WorkerPool) backoffDuration(attempt int) time.Duration {
	multiplier := time.Duration(1 << uint(attempt-1))
	return p.baseBackoff * multiplier
}

// processJob performs exactly one attempt.
func processJob(
	ctx context.Context,
	j job.Job,
	workerID int,
	attempt int,
) job.Result {

	start := time.Now()

	processingDuration := time.Second

	switch j.Payload {
	case "fail":
		processingDuration = time.Second

	case "slow":
		processingDuration = 5 * time.Second

	case "flaky":
		processingDuration = 300 * time.Millisecond
	}

	timer := time.NewTimer(processingDuration)
	defer timer.Stop()

	result := job.Result{
		JobID: j.ID,
	}

	select {
	case <-timer.C:

		switch j.Payload {
		case "fail":
			result.Status = job.StatusFailed
			result.Error = "simulated permanent failure"

		case "flaky":
			// This job fails twice, then succeeds.
			if attempt < 3 {
				result.Status = job.StatusFailed
				result.Error = "simulated temporary failure"
			} else {
				result.Status = job.StatusCompleted
			}

		default:
			result.Status = job.StatusCompleted
		}

	case <-ctx.Done():
		result.Status = job.StatusFailed
		result.Error = fmt.Sprintf(
			"job processing interrupted: %v",
			ctx.Err(),
		)
	}

	result.CompletedAt = time.Now()
	result.ProcessingTime = time.Since(start)

	fmt.Printf(
		"Worker %d finished job %d attempt %d: %s\n",
		workerID,
		j.ID,
		attempt,
		result.Status,
	)

	return result
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
