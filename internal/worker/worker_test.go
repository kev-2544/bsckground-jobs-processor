
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

	result := processJob(ctx, j, 1)

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

	result := processJob(ctx, j, 1)

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

	result := processJob(ctx, j, 1)

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
