package job

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

type Job struct {
	ID          int
	Name        string
	Payload     string
	Status      Status
	Attempts    int
	CreatedAt   time.Time
	CompletedAt time.Time
	Error       string
}

func NewJob(id int, name string, payload string) Job {
	return Job{
		ID:        id,
		Name:      name,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}
}
