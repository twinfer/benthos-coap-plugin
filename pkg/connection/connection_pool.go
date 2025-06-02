// pkg/connection/pool.go
package connection

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type ConnectionPool struct {
	endpoint     string
	protocol     string
	connections  chan *ConnectionWrapper
	factory      ConnectionFactory
	maxSize      int
	currentSize  int32
	security     SecurityConfig
	logger       *service.Logger
	metrics      *PoolMetrics
	healthTicker *time.Ticker
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.RWMutex
}

type PoolMetrics struct {
	PoolSize        *service.MetricCounter
	PoolUtilization *service.MetricCounter
	HealthChecks    *service.MetricCounter
	HealthFailures  *service.MetricCounter
}

type ConnectionWrapper struct {
	conn      interface{} // *udp.Conn, *tcp.Conn, etc.
	endpoint  string
	lastUsed  time.Time
	inUse     int32
	healthy   int32
	createdAt time.Time
	mu        sync.RWMutex
}

func NewConnectionPool(endpoint string, config Config, factory ConnectionFactory, logger *service.Logger, metrics *Metrics) (*ConnectionPool, error) {
	ctx, cancel := context.WithCancel(context.Background())

	poolMetrics := &PoolMetrics{
		PoolSize:        metrics.ConnectionsActive,
		PoolUtilization: metrics.ConnectionsCreated, // Reuse existing metric
		HealthChecks:    metrics.HealthChecksTotal,
		HealthFailures:  metrics.HealthChecksFailed,
	}

	pool := &ConnectionPool{
		endpoint:    endpoint,
		protocol:    config.Protocol,
		connections: make(chan *ConnectionWrapper, config.MaxPoolSize),
		factory:     factory,
		maxSize:     config.MaxPoolSize,
		security:    config.Security,
		logger:      logger,
		metrics:     poolMetrics,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start health checker if enabled
	if config.HealthCheckInterval > 0 {
		pool.healthTicker = time.NewTicker(config.HealthCheckInterval)
		go pool.healthChecker()
	}

	return pool, nil
}

func (p *ConnectionPool) Get(ctx context.Context) (*ConnectionWrapper, error) {
	// Try to get existing connection from pool
	select {
	case conn := <-p.connections:
		if atomic.LoadInt32(&conn.healthy) == 1 {
			atomic.StoreInt32(&conn.inUse, 1)
			conn.updateLastUsed()
			return conn, nil
		}
		// Connection unhealthy, close and try to create new
		p.closeConnection(conn)
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// No connections available in pool
	}

	// Create new connection if under limit
	if atomic.LoadInt32(&p.currentSize) >= int32(p.maxSize) {
		return nil, fmt.Errorf("connection pool full for endpoint %s", p.endpoint)
	}

	conn, err := p.createConnection()
	if err != nil {
		return nil, err
	}

	atomic.StoreInt32(&conn.inUse, 1)
	conn.updateLastUsed()
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
			p.closeConnection(conn)
		}
	} else {
		// Unhealthy connection, close it
		p.closeConnection(conn)
	}
}

func (p *ConnectionPool) createConnection() (*ConnectionWrapper, error) {
	conn, err := p.factory.Create(p.endpoint, p.security)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s: %w", p.endpoint, err)
	}

	wrapper := &ConnectionWrapper{
		conn:      conn,
		endpoint:  p.endpoint,
		lastUsed:  time.Now(),
		healthy:   1,
		createdAt: time.Now(),
	}

	atomic.AddInt32(&p.currentSize, 1)
	p.metrics.PoolSize.Incr(1)

	p.logger.Debug("Created new connection", "endpoint", p.endpoint, "pool_size", atomic.LoadInt32(&p.currentSize))
	return wrapper, nil
}

func (p *ConnectionPool) closeConnection(conn *ConnectionWrapper) {
	if conn == nil {
		return
	}

	p.factory.Close(conn.conn)
	atomic.AddInt32(&p.currentSize, -1)
	p.metrics.PoolSize.Incr(-1)

	p.logger.Debug("Closed connection", "endpoint", p.endpoint, "pool_size", atomic.LoadInt32(&p.currentSize))
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
	p.metrics.HealthChecks.Incr(1)

	// Check connections currently in pool
	poolSize := len(p.connections)
	checkedCount := 0
	failedCount := 0

	for i := 0; i < poolSize && checkedCount < 5; i++ { // Limit to 5 checks per cycle
		select {
		case conn := <-p.connections:
			checkedCount++

			if atomic.LoadInt32(&conn.inUse) == 0 {
				if err := p.factory.Validate(conn.conn); err != nil {
					p.logger.Debug("Connection failed health check",
						"endpoint", p.endpoint,
						"error", err,
						"connection_age", time.Since(conn.createdAt))

					atomic.StoreInt32(&conn.healthy, 0)
					p.closeConnection(conn)
					p.metrics.HealthFailures.Incr(1)
					failedCount++
				} else {
					// Return healthy connection to pool
					select {
					case p.connections <- conn:
					default:
						// Pool full, close connection
						p.closeConnection(conn)
					}
				}
			} else {
				// Return in-use connection without checking
				select {
				case p.connections <- conn:
				default:
					p.closeConnection(conn)
				}
			}
		default:
			break
		}
	}

	if failedCount > 0 {
		p.logger.Warn("Health check completed",
			"endpoint", p.endpoint,
			"checked", checkedCount,
			"failed", failedCount,
			"pool_size", atomic.LoadInt32(&p.currentSize))
	}
}

func (p *ConnectionPool) Close() error {
	p.cancel()

	if p.healthTicker != nil {
		p.healthTicker.Stop()
	}

	// Close all connections in pool
	close(p.connections)
	for conn := range p.connections {
		p.closeConnection(conn)
	}

	p.logger.Info("Connection pool closed", "endpoint", p.endpoint)
	return nil
}

func (p *ConnectionPool) Stats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"endpoint":     p.endpoint,
		"protocol":     p.protocol,
		"max_size":     p.maxSize,
		"current_size": atomic.LoadInt32(&p.currentSize),
		"pool_usage":   len(p.connections),
		"utilization":  float64(atomic.LoadInt32(&p.currentSize)) / float64(p.maxSize),
	}
}

// ConnectionWrapper methods
func (c *ConnectionWrapper) updateLastUsed() {
	c.mu.Lock()
	c.lastUsed = time.Now()
	c.mu.Unlock()
}

func (c *ConnectionWrapper) LastUsed() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastUsed
}

func (c *ConnectionWrapper) Age() time.Duration {
	return time.Since(c.createdAt)
}

func (c *ConnectionWrapper) IsHealthy() bool {
	return atomic.LoadInt32(&c.healthy) == 1
}

func (c *ConnectionWrapper) IsInUse() bool {
	return atomic.LoadInt32(&c.inUse) == 1
}
