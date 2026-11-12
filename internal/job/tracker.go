
package job

import (
	"fmt"
	"sync"
	"time"
)

// Tracker stores and manages the state of submitted jobs.
//
// Multiple workers may update jobs concurrently, so we protect
// the map with a read-write mutex.
type Tracker struct {
	mu   sync.RWMutex
	jobs map[int]Job
}

// NewTracker creates an initialized job tracker.
func NewTracker() *Tracker {
	return &Tracker{
		jobs: make(map[int]Job),
	}
}

// Register adds a newly submitted job to the tracker.
func (t *Tracker) Register(j Job) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.jobs[j.ID]; exists {
		return fmt.Errorf("job %d is already registered", j.ID)
	}

	t.jobs[j.ID] = j
	return nil
}

// UpdateStatus changes the status of an existing job.
func (t *Tracker) UpdateStatus(id int, status Status) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	j, exists := t.jobs[id]
	if !exists {
		return fmt.Errorf("job %d not found", id)
	}

	j.Status = status
	t.jobs[id] = j

	return nil
}

// Complete records the final outcome of a job.
func (t *Tracker) Complete(id int, status Status, processingError error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	j, exists := t.jobs[id]
	if !exists {
		return fmt.Errorf("job %d not found", id)
	}

	j.Status = status
	j.CompletedAt = time.Now()

	if processingError != nil {
		j.Error = processingError.Error()
	}

	t.jobs[id] = j

	return nil
}

// Get retrieves a job by ID.
func (t *Tracker) Get(id int) (Job, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	j, exists := t.jobs[id]
	return j, exists
}

// All returns a snapshot of all tracked jobs.
func (t *Tracker) All() []Job {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]Job, 0, len(t.jobs))

	for _, j := range t.jobs {
		result = append(result, j)
	}

	return result
}
