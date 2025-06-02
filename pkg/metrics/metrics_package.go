// pkg/metrics/metrics.go
package metrics

import (
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Manager provides centralized metrics collection for CoAP plugin
type Manager struct {
	resources *service.Resources

	// Connection metrics
	ConnectionsActive  *service.MetricCounter
	ConnectionsCreated *service.MetricCounter
	ConnectionsFailed  *service.MetricCounter
	ConnectionPoolSize *service.MetricGauge

	// Input metrics
	InputMessagesRead    *service.MetricCounter
	InputMessagesDropped *service.MetricCounter
	InputErrors          *service.MetricCounter
	InputLatency         *service.MetricTimer

	// Output metrics
	OutputMessagesSent   *service.MetricCounter
	OutputMessagesFailed *service.MetricCounter
	OutputRequests       *service.MetricCounter
	OutputLatency        *service.MetricTimer

	// Observer metrics
	ObservationsActive   *service.MetricGauge
	ObservationsTotal    *service.MetricCounter
	ObservationsFailed   *service.MetricCounter
	ResubscriptionsTotal *service.MetricCounter

	// Circuit breaker metrics
	CircuitBreakerOpen  *service.MetricCounter
	CircuitBreakerState *service.MetricGauge

	// Health metrics
	HealthChecksTotal  *service.MetricCounter
	HealthChecksFailed *service.MetricCounter

	// Performance metrics
	MessageProcessingDuration *service.MetricTimer
	PayloadSize               *service.MetricGauge

	mu sync.RWMutex
}

// NewManager creates a new metrics manager
func NewManager(resources *service.Resources) *Manager {
	return &Manager{
		resources: resources,

		// Connection metrics
		ConnectionsActive:  resources.MetricCounter("coap_connections_active"),
		ConnectionsCreated: resources.MetricCounter("coap_connections_created_total"),
		ConnectionsFailed:  resources.MetricCounter("coap_connections_failed_total"),
		ConnectionPoolSize: resources.MetricGauge("coap_connection_pool_size"),

		// Input metrics
		InputMessagesRead:    resources.MetricCounter("coap_input_messages_read_total"),
		InputMessagesDropped: resources.MetricCounter("coap_input_messages_dropped_total"),
		InputErrors:          resources.MetricCounter("coap_input_errors_total"),
		InputLatency:         resources.MetricTimer("coap_input_latency_seconds"),

		// Output metrics
		OutputMessagesSent:   resources.MetricCounter("coap_output_messages_sent_total"),
		OutputMessagesFailed: resources.MetricCounter("coap_output_messages_failed_total"),
		OutputRequests:       resources.MetricCounter("coap_output_requests_total"),
		OutputLatency:        resources.MetricTimer("coap_output_latency_seconds"),

		// Observer metrics
		ObservationsActive:   resources.MetricGauge("coap_observations_active"),
		ObservationsTotal:    resources.MetricCounter("coap_observations_total"),
		ObservationsFailed:   resources.MetricCounter("coap_observations_failed_total"),
		ResubscriptionsTotal: resources.MetricCounter("coap_resubscriptions_total"),

		// Circuit breaker metrics
		CircuitBreakerOpen:  resources.MetricCounter("coap_circuit_breaker_open_total"),
		CircuitBreakerState: resources.MetricGauge("coap_circuit_breaker_state"),

		// Health metrics
		HealthChecksTotal:  resources.MetricCounter("coap_health_checks_total"),
		HealthChecksFailed: resources.MetricCounter("coap_health_checks_failed_total"),

		// Performance metrics
		MessageProcessingDuration: resources.MetricTimer("coap_message_processing_duration_seconds"),
		PayloadSize:               resources.MetricGauge("coap_payload_size_bytes"),
	}
}

// ConnectionMetrics provides connection-related metrics
type ConnectionMetrics struct {
	manager *Manager
}

func (m *Manager) Connection() *ConnectionMetrics {
	return &ConnectionMetrics{manager: m}
}

func (c *ConnectionMetrics) IncActive(delta int) {
	c.manager.ConnectionsActive.Incr(int64(delta))
}

func (c *ConnectionMetrics) IncCreated() {
	c.manager.ConnectionsCreated.Incr(1)
}

func (c *ConnectionMetrics) IncFailed() {
	c.manager.ConnectionsFailed.Incr(1)
}

func (c *ConnectionMetrics) SetPoolSize(size int) {
	c.manager.ConnectionPoolSize.Set(int64(size))
}

// InputMetrics provides input-related metrics
type InputMetrics struct {
	manager *Manager
}

func (m *Manager) Input() *InputMetrics {
	return &InputMetrics{manager: m}
}

func (i *InputMetrics) IncMessagesRead() {
	i.manager.InputMessagesRead.Incr(1)
}

func (i *InputMetrics) IncMessagesDropped() {
	i.manager.InputMessagesDropped.Incr(1)
}

func (i *InputMetrics) IncErrors() {
	i.manager.InputErrors.Incr(1)
}

func (i *InputMetrics) RecordLatency(duration time.Duration) {
	i.manager.InputLatency.Timing(duration.Nanoseconds())
}

// OutputMetrics provides output-related metrics
type OutputMetrics struct {
	manager *Manager
}

func (m *Manager) Output() *OutputMetrics {
	return &OutputMetrics{manager: m}
}

func (o *OutputMetrics) IncMessagesSent() {
	o.manager.OutputMessagesSent.Incr(1)
}

func (o *OutputMetrics) IncMessagesFailed() {
	o.manager.OutputMessagesFailed.Incr(1)
}

func (o *OutputMetrics) IncRequests() {
	o.manager.OutputRequests.Incr(1)
}

func (o *OutputMetrics) RecordLatency(duration time.Duration) {
	o.manager.OutputLatency.Timing(duration.Nanoseconds())
}

// ObserverMetrics provides observer-related metrics
type ObserverMetrics struct {
	manager *Manager
}

func (m *Manager) Observer() *ObserverMetrics {
	return &ObserverMetrics{manager: m}
}

func (o *ObserverMetrics) SetActive(count int) {
	o.manager.ObservationsActive.Set(int64(count))
}

func (o *ObserverMetrics) IncTotal() {
	o.manager.ObservationsTotal.Incr(1)
}

func (o *ObserverMetrics) IncFailed() {
	o.manager.ObservationsFailed.Incr(1)
}

func (o *ObserverMetrics) IncResubscriptions() {
	o.manager.ResubscriptionsTotal.Incr(1)
}

// CircuitBreakerMetrics provides circuit breaker metrics
type CircuitBreakerMetrics struct {
	manager *Manager
}

func (m *Manager) CircuitBreaker() *CircuitBreakerMetrics {
	return &CircuitBreakerMetrics{manager: m}
}

func (c *CircuitBreakerMetrics) IncOpen() {
	c.manager.CircuitBreakerOpen.Incr(1)
}

func (c *CircuitBreakerMetrics) SetState(state int) {
	c.manager.CircuitBreakerState.Set(int64(state))
}

// HealthMetrics provides health check metrics
type HealthMetrics struct {
	manager *Manager
}

func (m *Manager) Health() *HealthMetrics {
	return &HealthMetrics{manager: m}
}

func (h *HealthMetrics) IncTotal() {
	h.manager.HealthChecksTotal.Incr(1)
}

func (h *HealthMetrics) IncFailed() {
	h.manager.HealthChecksFailed.Incr(1)
}

// PerformanceMetrics provides performance metrics
type PerformanceMetrics struct {
	manager *Manager
}

func (m *Manager) Performance() *PerformanceMetrics {
	return &PerformanceMetrics{manager: m}
}

func (p *PerformanceMetrics) RecordProcessingDuration(duration time.Duration) {
	p.manager.MessageProcessingDuration.Timing(duration.Nanoseconds())
}

func (p *PerformanceMetrics) SetPayloadSize(size int) {
	p.manager.PayloadSize.Set(int64(size))
}
