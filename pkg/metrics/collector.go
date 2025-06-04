// pkg/metrics/collector.go
package metrics

import (
	"context"
	"maps"
	"sync"
	"time"
)

// Collector aggregates metrics from multiple sources
type Collector struct {
	manager *Manager

	// Collected data
	data map[string]any
	mu   sync.RWMutex

	// Collection interval
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCollector creates a new metrics collector
func NewCollector(manager *Manager, interval time.Duration) *Collector {
	return &Collector{
		manager:  manager,
		data:     make(map[string]any),
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start begins metric collection
func (c *Collector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	defer close(c.doneCh)

	for {
		select {
		case <-ticker.C:
			c.collect()
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop stops metric collection
func (c *Collector) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

// GetData returns collected metrics data
func (c *Collector) GetData() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]any)
	maps.Copy(result, c.data)
	return result
}

func (c *Collector) collect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect timestamp
	c.data["collection_time"] = time.Now().Unix()

	// Note: In a real implementation, you would extract actual values
	// from the Benthos metrics. This is a simplified version.
	c.data["summary"] = map[string]any{
		"connections_active":    0, // Would get from actual metric
		"messages_processed":    0,
		"errors_total":          0,
		"observations_active":   0,
		"circuit_breakers_open": 0,
	}
}
