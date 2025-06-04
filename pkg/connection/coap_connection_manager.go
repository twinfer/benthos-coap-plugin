// pkg/connection/manager.go
package connection

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type Manager struct {
	pools        map[string]*ConnectionPool
	endpointMap  map[string]string // maps cleaned endpoint to original endpoint
	config       Config
	logger       *service.Logger
	metrics      *Metrics
	mu           sync.RWMutex
}

type Config struct {
	Endpoints           []string       `yaml:"endpoints"`
	Protocol            string         `yaml:"protocol"`
	MaxPoolSize         int            `yaml:"max_pool_size"`
	IdleTimeout         time.Duration  `yaml:"idle_timeout"`
	HealthCheckInterval time.Duration  `yaml:"health_check_interval"`
	ConnectTimeout      time.Duration  `yaml:"connect_timeout"`
	Security            SecurityConfig `yaml:"security"`
}

type SecurityConfig struct {
	Mode         string `yaml:"mode"`
	PSKIdentity  string `yaml:"psk_identity"`
	PSKKey       string `yaml:"psk_key"`
	CertFile     string `yaml:"cert_file"`
	KeyFile      string `yaml:"key_file"`
	CACertFile   string `yaml:"ca_cert_file"`
	InsecureSkip bool   `yaml:"insecure_skip_verify"`
}

type Metrics struct {
	ConnectionsActive  *service.MetricCounter
	ConnectionsCreated *service.MetricCounter
	ConnectionsFailed  *service.MetricCounter
	HealthChecksTotal  *service.MetricCounter
	HealthChecksFailed *service.MetricCounter
}

func NewManager(config Config, logger *service.Logger, metrics *service.Resources) (*Manager, error) {
	m := &Manager{
		pools:       make(map[string]*ConnectionPool),
		endpointMap: make(map[string]string),
		config:      config,
		logger:      logger,
		metrics: &Metrics{
			ConnectionsActive:  metrics.Metrics().NewCounter("coap_connections_active"),
			ConnectionsCreated: metrics.Metrics().NewCounter("coap_connections_created"),
			ConnectionsFailed:  metrics.Metrics().NewCounter("coap_connections_failed"),
			HealthChecksTotal:  metrics.Metrics().NewCounter("coap_health_checks_total"),
			HealthChecksFailed: metrics.Metrics().NewCounter("coap_health_checks_failed"),
		},
	}

	factory, err := m.createFactory()
	if err != nil {
		return nil, fmt.Errorf("failed to create connection factory: %w", err)
	}

	for _, endpoint := range config.Endpoints {
		// Strip scheme from endpoint for the factory
		cleanEndpoint := stripScheme(endpoint)
		pool, err := NewConnectionPool(cleanEndpoint, config, factory, logger, m.metrics)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool for %s: %w", endpoint, err)
		}
		// Use original endpoint as key to maintain compatibility
		m.pools[endpoint] = pool
		// Map cleaned endpoint back to original for Put operations
		m.endpointMap[cleanEndpoint] = endpoint
	}

	return m, nil
}

func (m *Manager) createFactory() (ConnectionFactory, error) {
	return CreateFactory(m.config.Protocol)
}

func (m *Manager) Get(ctx context.Context) (*ConnectionWrapper, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Simple round-robin selection
	for _, pool := range m.pools {
		conn, err := pool.Get(ctx)
		if err == nil {
			return conn, nil
		}
		m.logger.Warnf("Failed to get connection from pool, endpoint: %s, error: %v", pool.endpoint, err)
	}

	return nil, fmt.Errorf("no healthy connections available")
}

func (m *Manager) Put(conn *ConnectionWrapper) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Try to find pool using the connection's endpoint directly
	pool, exists := m.pools[conn.endpoint]
	if !exists {
		// If not found, check if it's a cleaned endpoint that maps to an original
		if originalEndpoint, mapped := m.endpointMap[conn.endpoint]; mapped {
			pool, exists = m.pools[originalEndpoint]
		}
	}

	if exists {
		pool.Put(conn)
	} else {
		m.logger.Warnf("Attempted to return connection to non-existent pool, endpoint: %s", conn.endpoint)
		// We need to get the factory to close the connection, but we can't access pool.factory if pool is nil
		// Let's create a temporary factory to close it properly
		if factory, err := m.createFactory(); err == nil {
			factory.Close(conn.conn)
		}
	}
}

func (m *Manager) Config() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for endpoint, pool := range m.pools {
		if err := pool.Close(); err != nil {
			m.logger.Error(fmt.Sprintf("Failed to close pool, endpoint: %s, error: %v", endpoint, err))
			lastErr = err
		}
	}
	return lastErr
}

// stripScheme removes the protocol scheme from an endpoint URL
// Examples: "coap://host:port" -> "host:port", "host:port" -> "host:port"
func stripScheme(endpoint string) string {
	// Handle coap:// and coaps:// schemes
	if strings.HasPrefix(endpoint, "coap://") {
		return strings.TrimPrefix(endpoint, "coap://")
	}
	if strings.HasPrefix(endpoint, "coaps://") {
		return strings.TrimPrefix(endpoint, "coaps://")
	}
	// Return as-is if no scheme found
	return endpoint
}
