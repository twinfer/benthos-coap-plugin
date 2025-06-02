// pkg/observer/manager.go
package observer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/mux"
	"github.com/plgd-dev/go-coap/v3/tcp"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

type Manager struct {
	subscriptions map[string]*Subscription
	connManager   *connection.Manager
	converter     *converter.Converter
	msgChan       chan *service.Message
	config        Config
	logger        *service.Logger
	metrics       *Metrics
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.RWMutex
	wg            sync.WaitGroup
}

type Config struct {
	ObservePaths     []string      `yaml:"observe_paths"`
	RetryPolicy      RetryPolicy   `yaml:"retry_policy"`
	CircuitBreaker   CircuitConfig `yaml:"circuit_breaker"`
	BufferSize       int           `yaml:"buffer_size"`
	ObserveTimeout   time.Duration `yaml:"observe_timeout"`
	ResubscribeDelay time.Duration `yaml:"resubscribe_delay"`
}

type RetryPolicy struct {
	MaxRetries      int           `yaml:"max_retries"`
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	Multiplier      float64       `yaml:"multiplier"`
	Jitter          bool          `yaml:"jitter"`
}

type CircuitConfig struct {
	Enabled          bool          `yaml:"enabled"`
	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`
	Timeout          time.Duration `yaml:"timeout"`
	HalfOpenMaxCalls int           `yaml:"half_open_max_calls"`
}

type Subscription struct {
	path         string
	conn         *connection.ConnectionWrapper
	observeToken message.Token
	retryCount   int32
	lastSeen     time.Time
	circuit      *CircuitBreaker
	cancel       context.CancelFunc
	healthy      int32
	mu           sync.RWMutex
}

type CircuitBreaker struct {
	state           int32 // 0: closed, 1: open, 2: half-open
	failures        int32
	successes       int32
	lastFailureTime time.Time
	config          CircuitConfig
	halfOpenCalls   int32
	mu              sync.RWMutex
}

type Metrics struct {
	ObservationsActive   *service.MetricCounter
	ObservationsTotal    *service.MetricCounter
	ObservationsFailed   *service.MetricCounter
	MessagesReceived     *service.MetricCounter
	ResubscriptionsTotal *service.MetricCounter
	CircuitBreakerOpen   *service.MetricCounter
}

const (
	CircuitClosed = iota
	CircuitOpen
	CircuitHalfOpen
)

func NewManager(config Config, connManager *connection.Manager, converter *converter.Converter, logger *service.Logger, metrics *service.Resources) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	bufferSize := config.BufferSize
	if bufferSize == 0 {
		bufferSize = 1000
	}

	m := &Manager{
		subscriptions: make(map[string]*Subscription),
		connManager:   connManager,
		converter:     converter,
		msgChan:       make(chan *service.Message, bufferSize),
		config:        config,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		metrics: &Metrics{
			ObservationsActive:   metrics.MetricCounter("coap_observations_active"),
			ObservationsTotal:    metrics.MetricCounter("coap_observations_total"),
			ObservationsFailed:   metrics.MetricCounter("coap_observations_failed"),
			MessagesReceived:     metrics.MetricCounter("coap_messages_received"),
			ResubscriptionsTotal: metrics.MetricCounter("coap_resubscriptions_total"),
			CircuitBreakerOpen:   metrics.MetricCounter("coap_circuit_breaker_open"),
		},
	}

	return m, nil
}

func (m *Manager) Start() error {
	for _, path := range m.config.ObservePaths {
		if err := m.Subscribe(path); err != nil {
			m.logger.Error("Failed to subscribe to path", "path", path, "error", err)
			continue
		}
	}
	return nil
}

func (m *Manager) Subscribe(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subscriptions[path]; exists {
		return fmt.Errorf("already subscribed to path: %s", path)
	}

	conn, err := m.connManager.Get(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	circuit := NewCircuitBreaker(m.config.CircuitBreaker)

	subCtx, cancel := context.WithCancel(m.ctx)
	subscription := &Subscription{
		path:    path,
		conn:    conn,
		circuit: circuit,
		cancel:  cancel,
		healthy: 1,
	}

	m.subscriptions[path] = subscription
	m.wg.Add(1)

	go m.observeWithRetry(subCtx, subscription)
	return nil
}

func (m *Manager) observeWithRetry(ctx context.Context, sub *Subscription) {
	defer m.wg.Done()
	defer m.connManager.Put(sub.conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !sub.circuit.CanExecute() {
			m.logger.Debug("Circuit breaker open, skipping observe", "path", sub.path)
			time.Sleep(m.config.CircuitBreaker.Timeout)
			continue
		}

		err := m.performObserve(ctx, sub)
		if err != nil {
			atomic.AddInt32(&sub.retryCount, 1)
			sub.circuit.RecordFailure()
			m.metrics.ObservationsFailed.Incr(1)

			m.logger.Warn("Observe failed, retrying",
				"path", sub.path,
				"error", err,
				"retry_count", atomic.LoadInt32(&sub.retryCount))

			if atomic.LoadInt32(&sub.retryCount) >= int32(m.config.RetryPolicy.MaxRetries) {
				m.logger.Error("Max retries exceeded for observe", "path", sub.path)
				atomic.StoreInt32(&sub.healthy, 0)
				return
			}

			delay := m.calculateBackoff(int(atomic.LoadInt32(&sub.retryCount)))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		} else {
			atomic.StoreInt32(&sub.retryCount, 0)
			sub.circuit.RecordSuccess()
		}
	}
}

func (m *Manager) performObserve(ctx context.Context, sub *Subscription) error {
	var coapConn interface{}

	switch conn := sub.conn.conn.(type) {
	case *udp.Conn:
		coapConn = conn
	case *tcp.Conn:
		coapConn = conn
	default:
		return fmt.Errorf("unsupported connection type")
	}

	observeCtx, cancel := context.WithTimeout(ctx, m.config.ObserveTimeout)
	defer cancel()

	// Create observe handler
	handler := func(w mux.ResponseWriter, r *mux.Message) {
		m.handleObserveMessage(sub.path, r)
	}

	// Perform observe based on connection type
	switch conn := coapConn.(type) {
	case *udp.Conn:
		obs, err := conn.Observe(observeCtx, sub.path, handler)
		if err != nil {
			return fmt.Errorf("failed to start UDP observe: %w", err)
		}
		sub.observeToken = obs.Token()
		m.metrics.ObservationsActive.Incr(1)
		m.metrics.ObservationsTotal.Incr(1)

		// Wait for context cancellation or observe completion
		<-observeCtx.Done()
		obs.Cancel(context.Background())

	case *tcp.Conn:
		obs, err := conn.Observe(observeCtx, sub.path, handler)
		if err != nil {
			return fmt.Errorf("failed to start TCP observe: %w", err)
		}
		sub.observeToken = obs.Token()
		m.metrics.ObservationsActive.Incr(1)
		m.metrics.ObservationsTotal.Incr(1)

		// Wait for context cancellation or observe completion
		<-observeCtx.Done()
		obs.Cancel(context.Background())
	}

	return nil
}

func (m *Manager) handleObserveMessage(path string, coapMsg *mux.Message) {
	sub := m.getSubscription(path)
	if sub == nil {
		m.logger.Warn("Received message for unknown subscription", "path", path)
		return
	}

	sub.lastSeen = time.Now()
	m.metrics.MessagesReceived.Incr(1)

	// Convert CoAP message to Benthos message
	msg, err := m.converter.CoAPToMessage(coapMsg.Message)
	if err != nil {
		m.logger.Error("Failed to convert CoAP message", "path", path, "error", err)
		return
	}

	// Add metadata
	msg.MetaSet("coap_path", path)
	msg.MetaSet("coap_token", string(coapMsg.Token()))
	msg.MetaSet("coap_observe", fmt.Sprintf("%d", coapMsg.Options().Observe()))

	select {
	case m.msgChan <- msg:
		// Message queued successfully
	default:
		m.logger.Warn("Message channel full, dropping message", "path", path)
	}
}

func (m *Manager) getSubscription(path string) *Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscriptions[path]
}

func (m *Manager) calculateBackoff(retryCount int) time.Duration {
	delay := float64(m.config.RetryPolicy.InitialInterval)

	for i := 0; i < retryCount; i++ {
		delay *= m.config.RetryPolicy.Multiplier
	}

	if time.Duration(delay) > m.config.RetryPolicy.MaxInterval {
		delay = float64(m.config.RetryPolicy.MaxInterval)
	}

	if m.config.RetryPolicy.Jitter {
		// Add up to 25% jitter
		jitter := delay * 0.25 * (2*float64(time.Now().UnixNano()%100)/100 - 1)
		delay += jitter
	}

	return time.Duration(delay)
}

func (m *Manager) MessageChan() <-chan *service.Message {
	return m.msgChan
}

func (m *Manager) Close() error {
	m.cancel()

	m.mu.Lock()
	for path, sub := range m.subscriptions {
		sub.cancel()
		m.logger.Debug("Cancelled subscription", "path", path)
	}
	m.mu.Unlock()

	m.wg.Wait()
	close(m.msgChan)

	return nil
}

func (m *Manager) HealthStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]interface{})
	healthyCount := 0

	for path, sub := range m.subscriptions {
		subStatus := map[string]interface{}{
			"healthy":       atomic.LoadInt32(&sub.healthy) == 1,
			"retry_count":   atomic.LoadInt32(&sub.retryCount),
			"last_seen":     sub.lastSeen.Format(time.RFC3339),
			"circuit_state": sub.circuit.State(),
		}
		status[path] = subStatus

		if atomic.LoadInt32(&sub.healthy) == 1 {
			healthyCount++
		}
	}

	status["summary"] = map[string]interface{}{
		"total_subscriptions":   len(m.subscriptions),
		"healthy_subscriptions": healthyCount,
		"buffer_usage":          fmt.Sprintf("%d/%d", len(m.msgChan), cap(m.msgChan)),
	}

	return status
}

// Circuit Breaker Implementation
func NewCircuitBreaker(config CircuitConfig) *CircuitBreaker {
	if !config.Enabled {
		return &CircuitBreaker{state: CircuitClosed, config: config}
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

	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		return time.Since(cb.lastFailureTime) >= cb.config.Timeout
	case CircuitHalfOpen:
		return atomic.LoadInt32(&cb.halfOpenCalls) < int32(cb.config.HalfOpenMaxCalls)
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	if !cb.config.Enabled {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		atomic.StoreInt32(&cb.failures, 0)
	case CircuitHalfOpen:
		atomic.AddInt32(&cb.successes, 1)
		if atomic.LoadInt32(&cb.successes) >= int32(cb.config.SuccessThreshold) {
			cb.state = CircuitClosed
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
	defer cb.mu.Unlock()

	atomic.AddInt32(&cb.failures, 1)
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case CircuitClosed:
		if atomic.LoadInt32(&cb.failures) >= int32(cb.config.FailureThreshold) {
			cb.state = CircuitOpen
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		atomic.StoreInt32(&cb.successes, 0)
		atomic.StoreInt32(&cb.halfOpenCalls, 0)
	}
}

func (cb *CircuitBreaker) State() string {
	if !cb.config.Enabled {
		return "disabled"
	}

	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) >= cb.config.Timeout {
			cb.state = CircuitHalfOpen
			return "half-open"
		}
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}
