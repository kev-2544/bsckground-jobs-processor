
package main

import (
	"fmt"

	"background-job-processor/internal/job"
	"background-job-processor/internal/queue"
	"background-job-processor/internal/worker"
)

func main() {
	// Configure our processor.
	const workerCount = 3
	const queueCapacity = 10

	// Create a buffered queue that can hold 10 jobs.
	jobQueue := queue.NewQueue(queueCapacity)

	// Give the worker pool access to the queue's receive-only channel.
	pool := worker.NewWorkerPool(workerCount, jobQueue.Jobs())

	// Start all workers before submitting jobs.
	pool.Start()

	// Submit six jobs to the queue.
	for i := 1; i <= 6; i++ {
		newJob := job.NewJob(
			i,
			fmt.Sprintf("Background Task %d", i),
			fmt.Sprintf("Payload for job %d", i),
		)

		if err := jobQueue.Submit(newJob); err != nil {
			fmt.Printf("Failed to submit job %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Submitted job %d\n", i)
	}

	// No more jobs will be submitted.
	// Workers will finish processing jobs already in the queue.
	jobQueue.Close()

	// Wait for every worker to finish.
	pool.Wait()

	fmt.Println("\nAll background jobs processed.")
}
