package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"background-job-processor/internal/job"
)

func TestProcessJobSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	j := job.NewJob(1, "Test Job", "normal")

	result := processJob(ctx, j, 1, 1)

	if result.Status != job.StatusCompleted {
		t.Fatalf("expected completed, got %s", result.Status)
	}

	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
}

func TestProcessJobFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	j := job.NewJob(2, "Failing Job", "fail")

	result := processJob(ctx, j, 1, 1)

	if result.Status != job.StatusFailed {
		t.Fatalf("expected failed, got %s", result.Status)
	}

	if result.Error == "" {
		t.Fatal("expected an error message")
	}
}

func TestProcessJobTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	j := job.NewJob(3, "Slow Job", "slow")

	result := processJob(ctx, j, 1, 1)

	if result.Status != job.StatusFailed {
		t.Fatalf("expected failed status, got %s", result.Status)
	}

	if !strings.Contains(result.Error, "deadline exceeded") {
		t.Fatalf("expected timeout error, got %s", result.Error)
	}

	if result.ProcessingTime >= 5*time.Second {
		t.Fatal("expected processing to stop before the simulated 5-second duration")
	}
}

func TestFlakyJobSucceedsAfterRetries(t *testing.T) {
	tracker := job.NewTracker()
	newJob := job.NewJob(1, "Flaky Job", "flaky")

	if err := tracker.Register(newJob); err != nil {
		t.Fatal(err)
	}

	results := make(chan job.Result, 1)
	deadLetters := make(chan job.DeadLetter, 1)
	jobs := make(chan job.Job, 1)

	pool := NewWorkerPool(
		1,
		2*time.Second,
		3,
		time.Millisecond,
		jobs,
		results,
		deadLetters,
		tracker,
	)

	pool.Start()
	jobs <- newJob
	close(jobs)
	pool.Wait()

	result := <-results

	if result.Status != job.StatusCompleted {
		t.Fatalf("expected completed, got %s", result.Status)
	}

	trackedJob, exists := tracker.Get(newJob.ID)
	if !exists {
		t.Fatal("expected job to exist in tracker")
	}

	if trackedJob.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", trackedJob.Attempts)
	}

	if trackedJob.Status != job.StatusCompleted {
		t.Fatalf("expected tracked status completed, got %s", trackedJob.Status)
	}

	if len(deadLetters) != 0 {
		t.Fatal("successful job should not enter the dead letter queue")
	}
}

func TestFailedJobMovesToDeadLetterQueue(t *testing.T) {
	tracker := job.NewTracker()
	newJob := job.NewJob(2, "Permanent Failure", "fail")

	if err := tracker.Register(newJob); err != nil {
		t.Fatal(err)
	}

	results := make(chan job.Result, 1)
	deadLetters := make(chan job.DeadLetter, 1)
	jobs := make(chan job.Job, 1)

	pool := NewWorkerPool(
		1,
		50*time.Millisecond,
		3,
		time.Millisecond,
		jobs,
		results,
		deadLetters,
		tracker,
	)

	pool.Start()
	jobs <- newJob
	close(jobs)
	pool.Wait()

	result := <-results
	deadLetter := <-deadLetters

	if result.Status != job.StatusDeadLetter {
		t.Fatalf("expected dead_letter, got %s", result.Status)
	}

	if deadLetter.Job.Attempts != 3 {
		t.Fatalf(
			"expected dead letter to record 3 attempts, got %d",
			deadLetter.Job.Attempts,
		)
	}

	if deadLetter.Job.Status != job.StatusDeadLetter {
		t.Fatalf(
			"expected dead letter job status dead_letter, got %s",
			deadLetter.Job.Status,
		)
	}
}
