
package job

import "time"

// Status represents the current state of a job.
type Status string

const (
	StatusPending   Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Job represents a single unit of background work.
type Job struct {
	ID          int
	Name        string
	Payload     string
	Status      Status
	CreatedAt   time.Time
	CompletedAt time.Time
	Error       string
}

// NewJob creates and initializes a new job.
func NewJob(id int, name string, payload string) Job {
	return Job{
		ID:        id,
		Name:      name,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}
}
