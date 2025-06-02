// pkg/observer/circuit_breaker.go
package observer

import (
	"sync"
	"sync/atomic"
	"time"
)

// Circuit breaker states
const (
	CircuitClosed = iota
	CircuitOpen
	CircuitHalfOpen
)

type CircuitBreaker struct {
	state           int32 // 0: closed, 1: open, 2: half-open
	failures        int32
	successes       int32
	lastFailureTime time.Time
	config          CircuitConfig
	halfOpenCalls   int32
	mu              sync.RWMutex
}

type CircuitConfig struct {
	Enabled          bool          `yaml:"enabled"`
	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`
	Timeout          time.Duration `yaml:"timeout"`
	HalfOpenMaxCalls int           `yaml:"half_open_max_calls"`
}

func NewCircuitBreaker(config CircuitConfig) *CircuitBreaker {
	if !config.Enabled {
		return &CircuitBreaker{
			state:  CircuitClosed,
			config: config,
		}
	}

	// Set defaults if not configured
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold == 0 {
		config.SuccessThreshold = 3
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.HalfOpenMaxCalls == 0 {
		config.HalfOpenMaxCalls = 2
	}

	return &CircuitBreaker{
		state:  CircuitClosed,
		config: config,
	}
}

func (cb *CircuitBreaker) CanExecute() bool {
	if !cb.config.Enabled {
		return true
	}

	currentState := atomic.LoadInt32(&cb.state)

	switch currentState {
	case CircuitClosed:
		return true

	case CircuitOpen:
		cb.mu.RLock()
		canTransition := time.Since(cb.lastFailureTime) >= cb.config.Timeout
		cb.mu.RUnlock()

		if canTransition {
			// Try to transition to half-open
			if atomic.CompareAndSwapInt32(&cb.state, CircuitOpen, CircuitHalfOpen) {
				atomic.StoreInt32(&cb.halfOpenCalls, 0)
				atomic.StoreInt32(&cb.successes, 0)
			}
			return true
		}
		return false

	case CircuitHalfOpen:
		currentCalls := atomic.LoadInt32(&cb.halfOpenCalls)
		if currentCalls < int32(cb.config.HalfOpenMaxCalls) {
			atomic.AddInt32(&cb.halfOpenCalls, 1)
			return true
		}
		return false

	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	if !cb.config.Enabled {
		return
	}

	currentState := atomic.LoadInt32(&cb.state)

	switch currentState {
	case CircuitClosed:
		// Reset failure count on success
		atomic.StoreInt32(&cb.failures, 0)

	case CircuitHalfOpen:
		successes := atomic.AddInt32(&cb.successes, 1)
		if successes >= int32(cb.config.SuccessThreshold) {
			// Transition to closed state
			atomic.StoreInt32(&cb.state, CircuitClosed)
			atomic.StoreInt32(&cb.failures, 0)
			atomic.StoreInt32(&cb.successes, 0)
			atomic.StoreInt32(&cb.halfOpenCalls, 0)
		}
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	cb.lastFailureTime = time.Now()
	cb.mu.Unlock()

	failures := atomic.AddInt32(&cb.failures, 1)
	currentState := atomic.LoadInt32(&cb.state)

	switch currentState {
	case CircuitClosed:
		if failures >= int32(cb.config.FailureThreshold) {
			atomic.StoreInt32(&cb.state, CircuitOpen)
		}

	case CircuitHalfOpen:
		// Any failure in half-open state transitions back to open
		atomic.StoreInt32(&cb.state, CircuitOpen)
		atomic.StoreInt32(&cb.successes, 0)
		atomic.StoreInt32(&cb.halfOpenCalls, 0)
	}
}

func (cb *CircuitBreaker) State() string {
	if !cb.config.Enabled {
		return "disabled"
	}

	currentState := atomic.LoadInt32(&cb.state)

	switch currentState {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		cb.mu.RLock()
		canTransition := time.Since(cb.lastFailureTime) >= cb.config.Timeout
		cb.mu.RUnlock()

		if canTransition {
			return "half-open-ready"
		}
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func (cb *CircuitBreaker) Stats() map[string]interface{} {
	if !cb.config.Enabled {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	cb.mu.RLock()
	lastFailure := cb.lastFailureTime
	cb.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled":             true,
		"state":               cb.State(),
		"failures":            atomic.LoadInt32(&cb.failures),
		"successes":           atomic.LoadInt32(&cb.successes),
		"failure_threshold":   cb.config.FailureThreshold,
		"success_threshold":   cb.config.SuccessThreshold,
		"timeout":             cb.config.Timeout.String(),
		"half_open_max_calls": cb.config.HalfOpenMaxCalls,
	}

	if !lastFailure.IsZero() {
		stats["last_failure"] = lastFailure.Format(time.RFC3339)
		stats["time_since_failure"] = time.Since(lastFailure).String()
	}

	if atomic.LoadInt32(&cb.state) == CircuitHalfOpen {
		stats["half_open_calls"] = atomic.LoadInt32(&cb.halfOpenCalls)
	}

	return stats
}

func (cb *CircuitBreaker) Reset() {
	if !cb.config.Enabled {
		return
	}

	atomic.StoreInt32(&cb.state, CircuitClosed)
	atomic.StoreInt32(&cb.failures, 0)
	atomic.StoreInt32(&cb.successes, 0)
	atomic.StoreInt32(&cb.halfOpenCalls, 0)

	cb.mu.Lock()
	cb.lastFailureTime = time.Time{}
	cb.mu.Unlock()
}

// CircuitBreakerManager manages multiple circuit breakers
type CircuitBreakerManager struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
}

func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

func (cbm *CircuitBreakerManager) GetOrCreate(name string, config CircuitConfig) *CircuitBreaker {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	if cb, exists := cbm.breakers[name]; exists {
		return cb
	}

	cb := NewCircuitBreaker(config)
	cbm.breakers[name] = cb
	return cb
}

func (cbm *CircuitBreakerManager) Get(name string) (*CircuitBreaker, bool) {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	cb, exists := cbm.breakers[name]
	return cb, exists
}

func (cbm *CircuitBreakerManager) Remove(name string) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	delete(cbm.breakers, name)
}

func (cbm *CircuitBreakerManager) List() []string {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	names := make([]string, 0, len(cbm.breakers))
	for name := range cbm.breakers {
		names = append(names, name)
	}
	return names
}

func (cbm *CircuitBreakerManager) Stats() map[string]interface{} {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	stats := make(map[string]interface{})
	openCount := 0
	halfOpenCount := 0
	closedCount := 0

	for name, cb := range cbm.breakers {
		cbStats := cb.Stats()
		stats[name] = cbStats

		if enabled, ok := cbStats["enabled"].(bool); ok && enabled {
			switch cbStats["state"].(string) {
			case "open", "half-open-ready":
				openCount++
			case "half-open":
				halfOpenCount++
			case "closed":
				closedCount++
			}
		}
	}

	stats["summary"] = map[string]interface{}{
		"total":     len(cbm.breakers),
		"open":      openCount,
		"half_open": halfOpenCount,
		"closed":    closedCount,
	}

	return stats
}

func (cbm *CircuitBreakerManager) ResetAll() {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	for _, cb := range cbm.breakers {
		cb.Reset()
	}
}
