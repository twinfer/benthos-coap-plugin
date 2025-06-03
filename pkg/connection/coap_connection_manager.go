// pkg/connection/manager.go
package connection

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type Manager struct {
	pools   map[string]*ConnectionPool
	config  Config
	logger  *service.Logger
	metrics *Metrics
	mu      sync.RWMutex
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
		pools:  make(map[string]*ConnectionPool),
		config: config,
		logger: logger,
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
		pool, err := NewConnectionPool(endpoint, config, factory, logger, m.metrics)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool for %s: %w", endpoint, err)
		}
		m.pools[endpoint] = pool
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
	pool, exists := m.pools[conn.endpoint]
	m.mu.RUnlock()

	if exists {
		pool.Put(conn)
	} else {
		m.logger.Warnf("Attempted to return connection to non-existent pool, endpoint: %s", conn.endpoint)
		if pool.factory != nil {
			pool.factory.Close(conn.conn)
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
