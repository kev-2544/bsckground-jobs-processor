package job

import "time"

// DeadLetter stores a job that exhausted all processing attempts.
type DeadLetter struct {
	Job         Job
	FinalResult Result
	FailedAt    time.Time
}
