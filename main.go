

package main

import (
	"fmt"
	"sync"

	"background-job-processor/internal/job"
	"background-job-processor/internal/queue"
	"background-job-processor/internal/worker"
)

func main() {
	const workerCount = 3
	const queueCapacity = 10

	// Initialize the application components.
	jobQueue := queue.NewQueue(queueCapacity)
	tracker := job.NewTracker()

	// Results channel carries completed and failed job results.
	// The collector will consume these results concurrently.
	results := make(chan job.Result)

	// Connect the worker pool to the queue, results channel and tracker.
	pool := worker.NewWorkerPool(
		workerCount,
		jobQueue.Jobs(),
		results,
		tracker,
	)

	// Start the result collector before workers can publish results.
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

	// Start all workers.
	pool.Start()

	// Submit jobs.
	for i := 1; i <= 8; i++ {
		payload := fmt.Sprintf("Payload for job %d", i)

		// Simulate a failed job.
		if i == 4 || i == 7 {
			payload = "fail"
		}

		newJob := job.NewJob(
			i,
			fmt.Sprintf("Background Task %d", i),
			payload,
		)

		// Register before submission so workers can track the job.
		if err := tracker.Register(newJob); err != nil {
			fmt.Printf("Registration failed: %v\n", err)
			continue
		}

		if err := jobQueue.Submit(newJob); err != nil {
			fmt.Printf("Submission failed: %v\n", err)
		}
	}

	// Signal that no additional jobs will be submitted.
	jobQueue.Close()

	// Wait until all workers have finished processing.
	pool.Wait()

	// All workers have exited, so nobody can send more results.
	close(results)

	// Wait for the result collector to finish.
	collectorWG.Wait()

	// Display final job statistics.
	fmt.Println("\n========== JOB SUMMARY ==========")

	var completed, failed int

	for _, j := range tracker.All() {
		fmt.Printf("Job %d: %s\n", j.ID, j.Status)

		switch j.Status {
		case job.StatusCompleted:
			completed++
		case job.StatusFailed:
			failed++
		}
	}

	fmt.Println("---------------------------------")
	fmt.Printf("Total jobs: %d\n", completed+failed)
	fmt.Printf("Completed: %d\n", completed)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Println("=================================")
}
