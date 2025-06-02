// pkg/connection/manager.go
package connection

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/plgd-dev/go-coap/v3/dtls"
	"github.com/plgd-dev/go-coap/v3/tcp"
	"github.com/plgd-dev/go-coap/v3/udp"
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
			ConnectionsActive:  metrics.NewCounter("coap_connections_active"),
			ConnectionsCreated: metrics.NewCounter("coap_connections_created"),
			ConnectionsFailed:  metrics.NewCounter("coap_connections_failed"),
			HealthChecksTotal:  metrics.NewCounter("coap_health_checks_total"),
			HealthChecksFailed: metrics.NewCounter("coap_health_checks_failed"),
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
	switch m.config.Protocol {
	case "udp":
		return &UDPFactory{}, nil
	case "tcp":
		return &TCPFactory{}, nil
	case "udp-dtls":
		return &DTLSFactory{}, nil
	case "tcp-tls":
		return &TCPTLSFactory{}, nil
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", m.config.Protocol)
	}
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
		m.logger.Warn("Failed to get connection from pool", "endpoint", pool.endpoint, "error", err)
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
		m.logger.Warn("Attempted to return connection to non-existent pool", "endpoint", conn.endpoint)
		if pool.factory != nil {
			pool.factory.Close(conn.conn)
		}
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for endpoint, pool := range m.pools {
		if err := pool.Close(); err != nil {
			m.logger.Error("Failed to close pool", "endpoint", endpoint, "error", err)
			lastErr = err
		}
	}
	return lastErr
}

func (p *ConnectionPool) Get(ctx context.Context) (*ConnectionWrapper, error) {
	select {
	case conn := <-p.connections:
		if atomic.LoadInt32(&conn.healthy) == 1 {
			atomic.StoreInt32(&conn.inUse, 1)
			conn.lastUsed = time.Now()
			return conn, nil
		}
		// Connection unhealthy, close and create new
		p.factory.Close(conn.conn)
		atomic.AddInt32(&p.currentSize, -1)
		p.metrics.ConnectionsActive.Incr(-1)
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// No connections available, create new if under limit
	}

	if atomic.LoadInt32(&p.currentSize) >= int32(p.maxSize) {
		return nil, fmt.Errorf("connection pool full for endpoint %s", p.endpoint)
	}

	conn, err := p.createConnection()
	if err != nil {
		p.metrics.ConnectionsFailed.Incr(1)
		return nil, err
	}

	atomic.StoreInt32(&conn.inUse, 1)
	conn.lastUsed = time.Now()
	return conn, nil
}

func (p *ConnectionPool) Put(conn *ConnectionWrapper) {
	if conn == nil {
		return
	}

	atomic.StoreInt32(&conn.inUse, 0)

	if atomic.LoadInt32(&conn.healthy) == 1 {
		select {
		case p.connections <- conn:
			// Successfully returned to pool
		default:
			// Pool full, close connection
			p.factory.Close(conn.conn)
			atomic.AddInt32(&p.currentSize, -1)
			p.metrics.ConnectionsActive.Incr(-1)
		}
	} else {
		// Unhealthy connection, close it
		p.factory.Close(conn.conn)
		atomic.AddInt32(&p.currentSize, -1)
		p.metrics.ConnectionsActive.Incr(-1)
	}
}

func (p *ConnectionPool) createConnection() (*ConnectionWrapper, error) {
	conn, err := p.factory.Create(p.endpoint, p.security)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %w", p.endpoint, err)
	}

	wrapper := &ConnectionWrapper{
		conn:     conn,
		endpoint: p.endpoint,
		lastUsed: time.Now(),
		healthy:  1,
	}

	atomic.AddInt32(&p.currentSize, 1)
	p.metrics.ConnectionsActive.Incr(1)
	p.metrics.ConnectionsCreated.Incr(1)

	return wrapper, nil
}

func (p *ConnectionPool) healthChecker() {
	defer p.healthTicker.Stop()

	for {
		select {
		case <-p.healthTicker.C:
			p.performHealthCheck()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *ConnectionPool) performHealthCheck() {
	p.metrics.HealthChecksTotal.Incr(1)

	// Check existing connections in pool
	poolSize := len(p.connections)
	for i := 0; i < poolSize; i++ {
		select {
		case conn := <-p.connections:
			if atomic.LoadInt32(&conn.inUse) == 0 {
				if err := p.factory.Validate(conn.conn); err != nil {
					p.logger.Debug("Connection failed health check", "endpoint", p.endpoint, "error", err)
					atomic.StoreInt32(&conn.healthy, 0)
					p.factory.Close(conn.conn)
					atomic.AddInt32(&p.currentSize, -1)
					p.metrics.ConnectionsActive.Incr(-1)
					p.metrics.HealthChecksFailed.Incr(1)
				} else {
					p.connections <- conn // Return healthy connection
				}
			} else {
				p.connections <- conn // Return in-use connection without checking
			}
		default:
			break
		}
	}
}

func (p *ConnectionPool) Close() error {
	p.cancel()

	close(p.connections)
	for conn := range p.connections {
		p.factory.Close(conn.conn)
	}

	return nil
}

// UDP Factory
type UDPFactory struct{}

func (f *UDPFactory) Create(endpoint string, security SecurityConfig) (interface{}, error) {
	return udp.Dial(endpoint)
}

func (f *UDPFactory) Validate(conn interface{}) error {
	udpConn, ok := conn.(*udp.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simple ping using empty confirmable message
	_, err := udpConn.Ping(ctx)
	return err
}

func (f *UDPFactory) Close(conn interface{}) error {
	if udpConn, ok := conn.(*udp.Conn); ok {
		return udpConn.Close()
	}
	return nil
}

// TCP Factory
type TCPFactory struct{}

func (f *TCPFactory) Create(endpoint string, security SecurityConfig) (interface{}, error) {
	return tcp.Dial(endpoint)
}

func (f *TCPFactory) Validate(conn interface{}) error {
	tcpConn, ok := conn.(*tcp.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := tcpConn.Ping(ctx)
	return err
}

func (f *TCPFactory) Close(conn interface{}) error {
	if tcpConn, ok := conn.(*tcp.Conn); ok {
		return tcpConn.Close()
	}
	return nil
}

// DTLS Factory
type DTLSFactory struct{}

func (f *DTLSFactory) Create(endpoint string, security SecurityConfig) (interface{}, error) {
	config := &dtls.Config{}

	switch security.Mode {
	case "psk":
		config.PSK = func(hint []byte) ([]byte, error) {
			return []byte(security.PSKKey), nil
		}
		config.PSKIdentityHint = []byte(security.PSKIdentity)
	case "certificate":
		cert, err := tls.LoadX509KeyPair(security.CertFile, security.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
		config.InsecureSkipVerify = security.InsecureSkip
	}

	return dtls.Dial(endpoint, config)
}

func (f *DTLSFactory) Validate(conn interface{}) error {
	dtlsConn, ok := conn.(*dtls.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := dtlsConn.Ping(ctx)
	return err
}

func (f *DTLSFactory) Close(conn interface{}) error {
	if dtlsConn, ok := conn.(*dtls.Conn); ok {
		return dtlsConn.Close()
	}
	return nil
}

// TCP-TLS Factory
type TCPTLSFactory struct{}

func (f *TCPTLSFactory) Create(endpoint string, security SecurityConfig) (interface{}, error) {
	config := &tls.Config{
		InsecureSkipVerify: security.InsecureSkip,
	}

	if security.CertFile != "" && security.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(security.CertFile, security.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	return tcp.DialTLS(endpoint, config)
}

func (f *TCPTLSFactory) Validate(conn interface{}) error {
	tcpConn, ok := conn.(*tcp.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := tcpConn.Ping(ctx)
	return err
}

func (f *TCPTLSFactory) Close(conn interface{}) error {
	if tcpConn, ok := conn.(*tcp.Conn); ok {
		return tcpConn.Close()
	}
	return nil
}
