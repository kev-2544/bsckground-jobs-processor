
package job

import "time"

// Result represents the outcome of processing a job.
type Result struct {
	JobID           int
	Status          Status
	Error           string
	ProcessingTime  time.Duration
	CompletedAt     time.Time
}
