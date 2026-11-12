
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

	// Listen for Ctrl+C and termination signals.
	shutdownCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	// Initialize application components.
	jobQueue := queue.NewQueue(queueCapacity)
	tracker := job.NewTracker()

	results := make(chan job.Result)

	pool := worker.NewWorkerPool(
		workerCount,
		jobTimeout,
		jobQueue.Jobs(),
		results,
		tracker,
	)

	// Start the result collector.
	var collectorWG sync.WaitGroup

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

	// Start workers.
	pool.Start()

	fmt.Println("Background job processor started.")
	fmt.Println("Press Ctrl+C to request graceful shutdown.")

	// Simulate submitting background jobs.
	for i := 1; i <= 12; i++ {

		// Stop accepting new jobs when shutdown is requested.
		select {
		case <-shutdownCtx.Done():
			fmt.Println("\nShutdown requested. Stopping submissions.")
			goto shutdown
		default:
		}

		payload := fmt.Sprintf("Payload for job %d", i)

		// Simulate normal failures and timeouts.
		if i == 4 {
			payload = "fail"
		}

		if i == 7 {
			payload = "slow"
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

		// Slow down submissions so Ctrl+C can be tested.
		select {
		case <-shutdownCtx.Done():
			fmt.Println("\nShutdown requested.")
			goto shutdown
		case <-time.After(250 * time.Millisecond):
		}
	}

shutdown:

	fmt.Println("\nClosing job queue...")

	// No more jobs will be accepted.
	jobQueue.Close()

	// Workers finish processing queued jobs and then exit.
	fmt.Println("Waiting for workers to finish...")
	pool.Wait()

	// No worker can publish additional results now.
	close(results)

	// Wait for the result collector to finish.
	collectorWG.Wait()

	// Display final statistics.
	fmt.Println("\n========== JOB SUMMARY ==========")

	var completed, failed, pending int

	for _, j := range tracker.All() {
		fmt.Printf("Job %d: %s\n", j.ID, j.Status)

		switch j.Status {
		case job.StatusCompleted:
			completed++
		case job.StatusFailed:
			failed++
		case job.StatusPending:
			pending++
		}
	}

	fmt.Println("---------------------------------")
	fmt.Printf("Total registered: %d\n", completed+failed+pending)
	fmt.Printf("Completed: %d\n", completed)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Printf("Pending: %d\n", pending)
	fmt.Println("=================================")

	fmt.Println("Background job processor stopped.")
}
