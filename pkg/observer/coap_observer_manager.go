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
	"github.com/twinfer/benthos-coap-plugin/pkg/utils"
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

type Metrics struct {
	ObservationsActive   *service.MetricCounter
	ObservationsTotal    *service.MetricCounter
	ObservationsFailed   *service.MetricCounter
	MessagesReceived     *service.MetricCounter
	ResubscriptionsTotal *service.MetricCounter
	CircuitBreakerOpen   *service.MetricCounter
}

func NewManager(config Config, connManager *connection.Manager, converter *converter.Converter, logger *service.Logger, metrics *service.Resources) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	bufferSize := config.BufferSize
	if bufferSize == 0 {
		logger.Debug("Using default buffer size for observer manager as config.BufferSize is 0")
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
			ObservationsActive:   metrics.Metrics().NewCounter("coap_observations_active"),
			ObservationsTotal:    metrics.Metrics().NewCounter("coap_observations_total"),
			ObservationsFailed:   metrics.Metrics().NewCounter("coap_observations_failed"),
			MessagesReceived:     metrics.Metrics().NewCounter("coap_messages_received"),
			ResubscriptionsTotal: metrics.Metrics().NewCounter("coap_resubscriptions_total"),
			CircuitBreakerOpen:   metrics.Metrics().NewCounter("coap_circuit_breaker_open"),
		},
	}

	return m, nil
}

func (m *Manager) Start() error {
	endpoints := m.connManager.Config().Endpoints
	for _, path := range m.config.ObservePaths {
		if err := m.Subscribe(path); err != nil {
			m.logger.Errorf("Failed to subscribe to path %s on endpoints %v: %v", path, endpoints, err)
			// Decide if one failed subscription should stop others. Currently, it continues.
			continue
		}
	}
	return nil
}

func (m *Manager) Subscribe(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	endpoints := m.connManager.Config().Endpoints // Get endpoints for logging context
	if _, exists := m.subscriptions[path]; exists {
		return fmt.Errorf("already subscribed to path %s on endpoints %v", path, endpoints)
	}

	conn, err := m.connManager.Get(m.ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection for path %s on endpoints %v: %w", path, endpoints, err)
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

	m.logger.Infof("Successfully subscribed to path %s on endpoint %s", path, sub.conn.Endpoint())
	go m.observeWithRetry(subCtx, subscription)
	return nil
}

func (m *Manager) observeWithRetry(ctx context.Context, sub *Subscription) {
	defer m.wg.Done()
	defer m.connManager.Put(sub.conn) // Ensure connection is returned to the pool

	endpoint := sub.conn.Endpoint() // Get endpoint for logging context

	for {
		select {
		case <-ctx.Done():
			m.logger.Debugf("Observe context cancelled for path %s on endpoint %s", sub.path, endpoint)
			return
		default:
		}

		if !sub.circuit.CanExecute() {
			m.logger.Debugf("Circuit breaker open for path %s on endpoint %s, skipping observe. State: %s", sub.path, endpoint, sub.circuit.State())
			m.metrics.CircuitBreakerOpen.Incr(1)
			// Wait for circuit breaker timeout or context cancellation
			select {
			case <-time.After(sub.circuit.ResetInterval()): // Use circuit's reset interval
				continue
			case <-ctx.Done():
				m.logger.Debugf("Observe context cancelled while circuit breaker was open for path %s on endpoint %s", sub.path, endpoint)
				return
			}
		}

		err := m.performObserve(ctx, sub)
		if err != nil {
			currentRetryCount := atomic.AddInt32(&sub.retryCount, 1)
			sub.circuit.RecordFailure()
			m.metrics.ObservationsFailed.Incr(1)

			m.logger.Warnf("Observe failed for path %s on endpoint %s (retry %d/%d): %v", sub.path, endpoint, currentRetryCount, m.config.RetryPolicy.MaxRetries, err)

			if currentRetryCount >= int32(m.config.RetryPolicy.MaxRetries) {
				m.logger.Errorf("Max retries (%d) exceeded for observe on path %s on endpoint %s. Marking as unhealthy.", m.config.RetryPolicy.MaxRetries, sub.path, endpoint)
				atomic.StoreInt32(&sub.healthy, 0)
				// Do not return immediately, let the loop check ctx.Done() or circuit breaker state
				// This allows for potential recovery if the context is not done and circuit breaker eventually closes.
			}

			backoffConfig := utils.BackoffConfig{
				InitialInterval: m.config.RetryPolicy.InitialInterval,
				MaxInterval:     m.config.RetryPolicy.MaxInterval,
				Multiplier:      m.config.RetryPolicy.Multiplier,
				Jitter:          m.config.RetryPolicy.Jitter,
			}
			delay := utils.CalculateBackoff(int(currentRetryCount), backoffConfig)
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
		return fmt.Errorf("unsupported connection type %T for path %s on endpoint %s", sub.conn.conn, sub.path, sub.conn.Endpoint())
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
			return fmt.Errorf("failed to start UDP observe for path %s on endpoint %s: %w", sub.path, sub.conn.Endpoint(), err)
		}
		// If we reach here, obs is valid.
		m.metrics.ObservationsActive.Incr(1) // Increment active observations
		defer func() {
			m.logger.Debugf("Cancelling UDP observe for path %s on endpoint %s (token: %s) via defer", sub.path, sub.conn.Endpoint(), obs.Token())
			cancelErr := obs.Cancel(context.Background()) // Use a new context for cancellation
			if cancelErr != nil {
				m.logger.Warnf("Error cancelling UDP observe (via defer) for path %s on endpoint %s: %v", sub.path, sub.conn.Endpoint(), cancelErr)
			}
			m.metrics.ObservationsActive.Decr(1) // Decrement active observations
		}()

		sub.observeToken = obs.Token()
		m.metrics.ObservationsTotal.Incr(1)
		m.logger.Infof("UDP Observe started for path %s on endpoint %s with token %s", sub.path, sub.conn.Endpoint(), obs.Token())

		// Wait for context cancellation or observe completion
		<-observeCtx.Done()
		m.logger.Debugf("UDP Observe context done for path %s on endpoint %s.", sub.path, sub.conn.Endpoint())
		// Cancellation and metric decrementing are handled by defer.

	case *tcp.Conn:
		obs, err := conn.Observe(observeCtx, sub.path, handler)
		if err != nil {
			return fmt.Errorf("failed to start TCP observe for path %s on endpoint %s: %w", sub.path, sub.conn.Endpoint(), err)
		}
		// If we reach here, obs is valid.
		m.metrics.ObservationsActive.Incr(1) // Increment active observations
		defer func() {
			m.logger.Debugf("Cancelling TCP observe for path %s on endpoint %s (token: %s) via defer", sub.path, sub.conn.Endpoint(), obs.Token())
			cancelErr := obs.Cancel(context.Background()) // Use a new context for cancellation
			if cancelErr != nil {
				m.logger.Warnf("Error cancelling TCP observe (via defer) for path %s on endpoint %s: %v", sub.path, sub.conn.Endpoint(), cancelErr)
			}
			m.metrics.ObservationsActive.Decr(1) // Decrement active observations
		}()

		sub.observeToken = obs.Token()
		m.metrics.ObservationsTotal.Incr(1)
		m.logger.Infof("TCP Observe started for path %s on endpoint %s with token %s", sub.path, sub.conn.Endpoint(), obs.Token())

		// Wait for context cancellation or observe completion
		<-observeCtx.Done()
		m.logger.Debugf("TCP Observe context done for path %s on endpoint %s.", sub.path, sub.conn.Endpoint())
		// Cancellation and metric decrementing are handled by defer.
	}

	// If the observe context was cancelled (e.g., timeout), return its error.
	// Otherwise, if it completed without external cancellation, it might indicate an issue with the observation itself (e.g. server stopped sending).
	if err := observeCtx.Err(); err != nil && err != context.Canceled {
		return fmt.Errorf("observe operation ended for path %s on endpoint %s: %w", sub.path, sub.conn.Endpoint(), err)
	}
	return nil
}

func (m *Manager) handleObserveMessage(path string, coapMsg *mux.Message) {
	sub := m.getSubscription(path)
	if sub == nil {
		// This can happen if a message arrives after a subscription is cancelled/removed.
		m.logger.Warnf("Received message for unknown or removed subscription path: %s. Token: %s", path, coapMsg.Token())
		return
	}
	endpoint := sub.conn.Endpoint() // Get endpoint for logging context

	sub.lastSeen = time.Now()
	m.metrics.MessagesReceived.Incr(1)
	atomic.StoreInt32(&sub.healthy, 1) // Mark as healthy on receiving a message
	sub.circuit.RecordSuccess()      // Also record success in circuit breaker

	// Convert CoAP message to Benthos message
	msg, err := m.converter.CoAPToMessage(coapMsg.Message)
	if err != nil {
		m.logger.Errorf("Failed to convert CoAP message from path %s on endpoint %s: %v. Payload: %s", path, endpoint, err, string(coapMsg.Body()))
		return
	}

	// Add metadata
	msg.MetaSet("coap_path", path)
	msg.MetaSet("coap_token", string(coapMsg.Token()))
	observeVal, err := coapMsg.Options().Observe()
	if err == nil {
		msg.MetaSet("coap_observe", fmt.Sprintf("%d", observeVal))
	} else {
		msg.MetaSet("coap_observe", "unknown")
	}

	select {
	case m.msgChan <- msg:
		m.logger.Debugf("Successfully queued message from path %s on endpoint %s. Token: %s", path, endpoint, coapMsg.Token())
	default:
		m.logger.Warnf("Message channel full for observer manager (endpoints %v, paths %v), dropping message from path %s on endpoint %s. Token: %s", m.connManager.Config().Endpoints, m.config.ObservePaths, path, endpoint, coapMsg.Token())
	}
}

func (m *Manager) getSubscription(path string) *Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscriptions[path]
}

func (m *Manager) MessageChan() <-chan *service.Message {
	return m.msgChan
}

func (m *Manager) Close() error {
	m.logger.Info("Closing observer manager...")
	m.cancel() // Signal all observeWithRetry goroutines to stop

	m.mu.Lock() // Ensure exclusive access to subscriptions map
	m.logger.Debugf("Cancelling %d active subscriptions...", len(m.subscriptions))
	for path, sub := range m.subscriptions {
		endpoint := "unknown"
		if sub.conn != nil { // Connection might be nil if subscription failed early
			endpoint = sub.conn.Endpoint()
		}
		sub.cancel() // Cancel the context for this specific subscription
		m.logger.Debugf("Cancelled subscription for path %s on endpoint %s", path, endpoint)
	}
	m.mu.Unlock()

	m.logger.Debug("Waiting for all observer goroutines to complete...")
	m.wg.Wait() // Wait for all observeWithRetry goroutines to finish
	m.logger.Debug("All observer goroutines completed.")

	close(m.msgChan)
	m.logger.Info("Observer manager closed.")
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
	endpoints := []string{}
	if m.connManager != nil && m.connManager.Config().Endpoints != nil {
		endpoints = m.connManager.Config().Endpoints
	}

	status["summary"] = map[string]interface{}{
		"endpoints":             endpoints,
		"observe_paths":         m.config.ObservePaths,
		"total_subscriptions":   len(m.subscriptions),
		"healthy_subscriptions": healthyCount,
		"buffer_usage":          fmt.Sprintf("%d/%d", len(m.msgChan), cap(m.msgChan)),
	}

	return status
}
