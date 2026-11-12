package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"background-job-processor/internal/job"
	"background-job-processor/internal/queue"
	"background-job-processor/internal/worker"
)

func main() {
	const workerCount = 3
	const queueCapacity = 10
	const jobTimeout = 3 * time.Second
	const maxAttempts = 3
	const baseBackoff = 500 * time.Millisecond

	shutdownCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	jobQueue := queue.NewQueue(queueCapacity)
	tracker := job.NewTracker()

	results := make(chan job.Result)
	deadLetters := make(chan job.DeadLetter)

	pool := worker.NewWorkerPool(
		workerCount,
		jobTimeout,
		maxAttempts,
		baseBackoff,
		jobQueue.Jobs(),
		results,
		deadLetters,
		tracker,
	)

	var collectorWG sync.WaitGroup

	// Collect normal processing results.
	collectorWG.Add(1)
	go func() {
		defer collectorWG.Done()

		for result := range results {
			fmt.Printf(
				"\n[RESULT] Job %d | Status: %s | Duration: %v",
				result.JobID,
				result.Status,
				result.ProcessingTime,
			)

			if result.Error != "" {
				fmt.Printf(" | Error: %s", result.Error)
			}

			fmt.Println()
		}
	}()

	// Collect permanently failed jobs.
	collectorWG.Add(1)
	go func() {
		defer collectorWG.Done()

		for dead := range deadLetters {
			fmt.Printf(
				"\n[DEAD LETTER] Job %d exhausted its attempts. Reason: %s\n",
				dead.Job.ID,
				dead.FinalResult.Error,
			)
		}
	}()

	pool.Start()

	fmt.Println("Background job processor started.")
	fmt.Println("Press Ctrl+C to request graceful shutdown.")

	for i := 1; i <= 12; i++ {
		select {
		case <-shutdownCtx.Done():
			fmt.Println("\nShutdown requested.")
			goto shutdown
		default:
		}

		payload := fmt.Sprintf("Payload for job %d", i)

		switch i {
		case 4:
			payload = "fail"
		case 7:
			payload = "slow"
		case 9:
			payload = "flaky"
		}

		newJob := job.NewJob(
			i,
			fmt.Sprintf("Background Task %d", i),
			payload,
		)

		if err := tracker.Register(newJob); err != nil {
			fmt.Printf("Registration failed: %v\n", err)
			continue
		}

		if err := jobQueue.Submit(newJob); err != nil {
			fmt.Printf("Submission failed: %v\n", err)
		} else {
			fmt.Printf("Submitted job %d\n", i)
		}

		select {
		case <-shutdownCtx.Done():
			fmt.Println("\nShutdown requested.")
			goto shutdown

		case <-time.After(250 * time.Millisecond):
		}
	}

shutdown:
	fmt.Println("\nClosing job queue...")
	jobQueue.Close()

	fmt.Println("Waiting for workers to finish...")
	pool.Wait()

	// Workers have stopped sending, so both channels can now close.
	close(results)
	close(deadLetters)

	collectorWG.Wait()

	fmt.Println("\n========== JOB SUMMARY ==========")

	var completed, failed, dead, pending int

	for _, j := range tracker.All() {
		fmt.Printf("Job %d: %s | Attempts: %d\n", j.ID, j.Status, j.Attempts)

		switch j.Status {
		case job.StatusCompleted:
			completed++
		case job.StatusFailed:
			failed++
		case job.StatusDeadLetter:
			dead++
		case job.StatusPending:
			pending++
		}
	}

	fmt.Println("---------------------------------")
	fmt.Printf("Completed: %d\n", completed)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Printf("Dead letter: %d\n", dead)
	fmt.Printf("Pending: %d\n", pending)
	fmt.Println("=================================")
	fmt.Println("Background job processor stopped.")
}
