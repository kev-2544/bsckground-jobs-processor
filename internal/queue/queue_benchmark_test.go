package queue

import (
	"testing"

	"background-job-processor/internal/job"
)

func BenchmarkQueueSubmitReceive(b *testing.B) {
	q := NewQueue(1)

	j := job.NewJob(1, "Benchmark Job", "payload")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := q.Submit(j); err != nil {
			b.Fatal(err)
		}

		// Receive immediately so the one-slot buffer is available
		// for the next submission.
		<-q.Jobs()
	}
}
