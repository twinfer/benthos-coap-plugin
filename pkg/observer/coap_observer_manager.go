// pkg/observer/manager.go
package observer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"
	udpClient "github.com/plgd-dev/go-coap/v3/udp/client"
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
	closed        int32 // atomic flag to prevent double-close
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
	path            string
	conn            *connection.ConnectionWrapper
	observeToken    message.Token
	retryCount      int32
	lastSeen        time.Time
	circuit         *CircuitBreaker
	cancel          context.CancelFunc
	healthy         int32
	mu              sync.RWMutex
	coapObservation interface{} // Stores client.Observation from the observe call
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

	// Validate configuration
	bufferSize := config.BufferSize
	if bufferSize <= 0 {
		cancel() // Prevent context leak
		return nil, fmt.Errorf("buffer size must be positive, got: %d", bufferSize)
	}

	// Validate observe paths
	for _, path := range config.ObservePaths {
		if path == "" {
			cancel() // Prevent context leak
			return nil, fmt.Errorf("observe path cannot be empty")
		}
	}

	// Validate retry policy
	if config.RetryPolicy.Multiplier > 0 && config.RetryPolicy.Multiplier < 1.0 {
		cancel() // Prevent context leak
		return nil, fmt.Errorf("retry multiplier must be >= 1.0, got: %f", config.RetryPolicy.Multiplier)
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

	m.logger.Infof("Successfully subscribed to path %s on endpoint %s", path, subscription.conn.Endpoint())
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
			case <-time.After(m.config.ResubscribeDelay): // Use circuit's reset interval
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
	observeCtx, observeCancel := context.WithCancel(ctx) // Use a cancellable context for the observation itself
	defer observeCancel()

	m.logger.Infof("Starting observe for path %s on endpoint %s", sub.path, sub.conn.Endpoint())
	m.metrics.ObservationsTotal.Incr(1)

	var obsErr error

	notificationHandler := func(notification *pool.Message) {
		// Convert pool.Message to message.Message for the converter
		// The converter expects *message.Message
		msg := &message.Message{
			Code:    notification.Code(),
			Token:   notification.Token(),
			Options: notification.Options(),
		}

		// Copy payload from body
		if body := notification.Body(); body != nil {
			payload, err := io.ReadAll(body)
			if err == nil {
				msg.Payload = payload
			}
			body.Seek(0, 0) // Reset for any future reads
		}

		m.handleObserveMessage(sub.path, msg)
	}

	rawConn := sub.conn.Connection()
	protocol := sub.conn.Protocol()

	switch conn := rawConn.(type) {
	case *udpClient.Conn: // Handles UDP and DTLS
		observation, obsErr := conn.Observe(observeCtx, sub.path, notificationHandler)
		if obsErr == nil {
			sub.mu.Lock()
			if tokenProvider, ok := observation.(interface{ Token() message.Token }); ok {
				sub.observeToken = tokenProvider.Token()
			}
			sub.coapObservation = observation
			sub.mu.Unlock()
			m.logger.Infof("Observation started for path %s on %s (UDP/DTLS)", sub.path, sub.conn.Endpoint())
		}
	case *tcpClient.Conn: // Handles TCP and TCP-TLS
		observation, obsErr := conn.Observe(observeCtx, sub.path, notificationHandler)
		if obsErr == nil {
			sub.mu.Lock()
			if tokenProvider, ok := observation.(interface{ Token() message.Token }); ok {
				sub.observeToken = tokenProvider.Token()
			}
			sub.coapObservation = observation
			sub.mu.Unlock()
			m.logger.Infof("Observation started for path %s on %s (TCP/TLS)", sub.path, sub.conn.Endpoint())
		}
	default:
		obsErr = fmt.Errorf("unsupported connection type for observe: %T (protocol: %s)", rawConn, protocol)
	}

	if obsErr != nil {
		m.logger.Errorf("Failed to start observation for path %s on %s: %v", sub.path, sub.conn.Endpoint(), obsErr)
		return obsErr
	}

	// Wait for the observation to end, either by context cancellation or error within the observation
	// For client.Conn (UDP/DTLS), Observe blocks until observeCtx is cancelled or an error occurs.
	// For coapTCPClient.Conn, Observe also blocks in a similar manner.
	// The error handling for ongoing observation issues (like connection drops) is typically managed
	// by the go-coap library itself by cancelling the observeCtx or returning an error from the Observe() call.

	<-observeCtx.Done() // Wait until the observation context is done
	err := observeCtx.Err()

	sub.mu.Lock()
	obsObj := sub.coapObservation
	sub.mu.Unlock()

	// Cancel the observation if it provides a Cancel method
	if obsObj != nil {
		if cancellable, ok := obsObj.(interface{ Cancel(context.Context) error }); ok {
			cancelErr := cancellable.Cancel(context.Background())
			if cancelErr != nil {
				m.logger.Warnf("Error cancelling observation for path %s on %s: %v", sub.path, sub.conn.Endpoint(), cancelErr)
			} else {
				m.logger.Infof("Observation cancelled for path %s on %s", sub.path, sub.conn.Endpoint())
			}
		}
	}

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		m.logger.Errorf("Observation for path %s on %s ended with error: %v", sub.path, sub.conn.Endpoint(), err)
		return err // Return the error that caused the observation to stop
	}

	m.logger.Infof("Observation ended for path %s on %s (context error: %v)", sub.path, sub.conn.Endpoint(), err)
	return nil // If context was cancelled or deadline exceeded, it's not necessarily an error to propagate for retry logic.
}

// handleObserveMessage is called when a CoAP notification is received.
// The coapMsg here is expected to be *message.Message from go-coap/v3 library.
func (m *Manager) handleObserveMessage(path string, coapMsg *message.Message) {
	sub := m.getSubscription(path)
	if sub == nil {
		m.logger.Warnf("Received message for unknown or removed subscription path: %s. Token: %s", path, string(coapMsg.Token))
		return
	}

	var endpoint string
	if sub.conn != nil {
		endpoint = sub.conn.Endpoint()
	} else {
		endpoint = "unknown"
	}

	sub.lastSeen = time.Now()
	m.metrics.MessagesReceived.Incr(1)
	atomic.StoreInt32(&sub.healthy, 1)
	sub.circuit.RecordSuccess()

	// Convert CoAP message to Benthos message using the converter
	if m.converter == nil {
		m.logger.Errorf("Converter is nil, cannot convert CoAP message for path %s on %s", path, endpoint)
		return
	}

	benthosMsg, err := m.converter.CoAPToMessage(coapMsg)
	if err != nil {
		m.logger.Errorf("Failed to convert CoAP message to Benthos message for path %s on %s: %v", path, endpoint, err)
		return
	}

	// Add extra metadata not handled by the generic converter, if any.
	// The converter should ideally handle Token, Observe option, etc.
	// For now, let's assume converter.CoAPToMessage populates essential fields.
	// If specific metadata like token or observe sequence needs to be explicitly added here,
	// it can be done:
	// benthosMsg.MetaSet("coap_token", coapMsg.Token().String()) // Token might be bytes
	// if obsVal, err := coapMsg.Observe(); err == nil {
	//    benthosMsg.MetaSet("coap_observe_seq", fmt.Sprintf("%d", obsVal))
	// }

	select {
	case m.msgChan <- benthosMsg:
		m.logger.Debugf("Successfully queued message from path %s on %s. Token: %s", path, endpoint, string(coapMsg.Token))
	default:
		m.logger.Warnf("Message channel full for observer manager, dropping message from path %s on %s. Token: %s", path, endpoint, string(coapMsg.Token))
	}
}

func (m *Manager) getSubscription(path string) *Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscriptions[path]
}

func (m *Manager) Config() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func (m *Manager) MessageChan() <-chan *service.Message {
	return m.msgChan
}

func (m *Manager) Close() error {
	// Use atomic compare-and-swap to ensure Close is only executed once
	if !atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		m.logger.Debug("Observer manager already closed, skipping")
		return nil
	}

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

		sub.mu.RLock()
		obsObj := sub.coapObservation
		sub.mu.RUnlock()

		// Cancel the observation if it provides a Cancel method
		if obsObj != nil {
			if cancellable, ok := obsObj.(interface{ Cancel(context.Context) error }); ok {
				cancelErr := cancellable.Cancel(context.Background())
				if cancelErr != nil {
					m.logger.Warnf("Error cancelling observation during manager close for path %s on %s: %v", path, endpoint, cancelErr)
				} else {
					m.logger.Debugf("Observation cancelled during manager close for path %s on %s", path, endpoint)
				}
			}
		}
		m.logger.Debugf("Cancelled subscription and any active CoAP observation for path %s on endpoint %s", path, endpoint)
	}
	m.mu.Unlock()

	m.logger.Debug("Waiting for all observer goroutines to complete...")
	m.wg.Wait() // Wait for all observeWithRetry goroutines to finish
	m.logger.Debug("All observer goroutines completed.")

	close(m.msgChan)
	m.logger.Info("Observer manager closed.")

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
