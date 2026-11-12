
package queue

import (
	"testing"

	"background-job-processor/internal/job"
)

func TestSubmitJob(t *testing.T) {
	q := NewQueue(2)

	j := job.NewJob(1, "Generate Report", "Monthly report")

	err := q.Submit(j)

	if err != nil {
		t.Fatalf("expected successful submission, got: %v", err)
	}

	if q.Length() != 1 {
		t.Fatalf("expected queue length 1, got %d", q.Length())
	}
}

func TestQueueFull(t *testing.T) {
	q := NewQueue(1)

	first := job.NewJob(1, "Job One", "Payload")
	second := job.NewJob(2, "Job Two", "Payload")

	if err := q.Submit(first); err != nil {
		t.Fatal(err)
	}

	// The queue has capacity 1 and is already full.
	err := q.Submit(second)

	if err == nil {
		t.Fatal("expected an error when submitting to a full queue")
	}
}

func TestReceiveJob(t *testing.T) {
	q := NewQueue(2)

	expected := job.NewJob(1, "Send Email", "Welcome email")

	if err := q.Submit(expected); err != nil {
		t.Fatal(err)
	}

	received := <-q.Jobs()

	if received.ID != expected.ID {
		t.Fatalf("expected job ID %d, got %d", expected.ID, received.ID)
	}
}


func TestSubmitAfterClose(t *testing.T) {
	q := NewQueue(2)

	q.Close()

	j := job.NewJob(1, "Test Job", "payload")

	err := q.Submit(j)

	if err == nil {
		t.Fatal("expected submission to fail after queue closure")
	}
}

func TestCloseIsSafeToCallMultipleTimes(t *testing.T) {
	q := NewQueue(2)

	q.Close()
	q.Close()

	if q.Length() != 0 {
		t.Fatal("expected empty queue after closure")
	}
}


