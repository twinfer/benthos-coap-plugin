// pkg/metrics/timer.go
package metrics

import (
	"time"
)

// Timer provides timing utilities for metrics
type Timer struct {
	start time.Time
	name  string
}

// StartTimer creates and starts a new timer
func StartTimer(name string) *Timer {
	return &Timer{
		start: time.Now(),
		name:  name,
	}
}

// Stop stops the timer and records the duration
func (t *Timer) Stop(manager *Manager) {
	duration := time.Since(t.start)

	switch t.name {
	case "input_processing":
		manager.Performance().RecordProcessingDuration(duration)
	case "output_sending":
		manager.Output().RecordLatency(duration)
	case "input_reading":
		manager.Input().RecordLatency(duration)
	}
}

// Duration returns the elapsed time
func (t *Timer) Duration() time.Duration {
	return time.Since(t.start)
}
