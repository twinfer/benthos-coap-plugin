package connection

// MetricRecorder provides an interface for recording metrics
// This allows for easier testing by decoupling from Benthos types
type MetricRecorder interface {
	RecordConnectionCreated()
	RecordConnectionFailed()
	RecordConnectionActive(delta int64)
	RecordHealthCheck(success bool)
}

// BenthosMetricRecorder implements MetricRecorder using Benthos metrics
type BenthosMetricRecorder struct {
	metrics *Metrics
}

func NewBenthosMetricRecorder(metrics *Metrics) *BenthosMetricRecorder {
	return &BenthosMetricRecorder{metrics: metrics}
}

func (r *BenthosMetricRecorder) RecordConnectionCreated() {
	if r.metrics != nil && r.metrics.ConnectionsCreated != nil {
		r.metrics.ConnectionsCreated.Incr(1)
	}
}

func (r *BenthosMetricRecorder) RecordConnectionFailed() {
	if r.metrics != nil && r.metrics.ConnectionsFailed != nil {
		r.metrics.ConnectionsFailed.Incr(1)
	}
}

func (r *BenthosMetricRecorder) RecordConnectionActive(delta int64) {
	if r.metrics != nil && r.metrics.ConnectionsActive != nil {
		r.metrics.ConnectionsActive.Incr(delta)
	}
}

func (r *BenthosMetricRecorder) RecordHealthCheck(success bool) {
	if r.metrics != nil && r.metrics.HealthChecksTotal != nil {
		r.metrics.HealthChecksTotal.Incr(1)
	}
	if !success && r.metrics != nil && r.metrics.HealthChecksFailed != nil {
		r.metrics.HealthChecksFailed.Incr(1)
	}
}

// MockMetricRecorder for testing
type MockMetricRecorder struct {
	ConnectionsCreated int64
	ConnectionsFailed  int64
	ConnectionsActive  int64
	HealthChecksTotal  int64
	HealthChecksFailed int64
}

func (m *MockMetricRecorder) RecordConnectionCreated() {
	m.ConnectionsCreated++
}

func (m *MockMetricRecorder) RecordConnectionFailed() {
	m.ConnectionsFailed++
}

func (m *MockMetricRecorder) RecordConnectionActive(delta int64) {
	m.ConnectionsActive += delta
}

func (m *MockMetricRecorder) RecordHealthCheck(success bool) {
	m.HealthChecksTotal++
	if !success {
		m.HealthChecksFailed++
	}
}
