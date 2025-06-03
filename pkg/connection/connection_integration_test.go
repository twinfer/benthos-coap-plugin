package connection

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionPoolWithBenthosMocks(t *testing.T) {
	// Use Benthos MockResources which provides proper metric implementations
	resources := service.MockResources()

	// Create a proper logger from mock resources
	logger := resources.Logger()

	config := Config{
		MaxPoolSize:         3,
		IdleTimeout:         time.Minute,
		HealthCheckInterval: 10 * time.Second,
		Protocol:            "udp",
	}

	// Create metrics using the mocked resources
	metrics := &Metrics{
		ConnectionsActive:  resources.Metrics().NewCounter("test_connections_active"),
		ConnectionsCreated: resources.Metrics().NewCounter("test_connections_created"),
		ConnectionsFailed:  resources.Metrics().NewCounter("test_connections_failed"),
		HealthChecksTotal:  resources.Metrics().NewCounter("test_health_checks_total"),
		HealthChecksFailed: resources.Metrics().NewCounter("test_health_checks_failed"),
	}

	factory := &UDPFactory{}

	t.Run("Create and use pool", func(t *testing.T) {
		pool, err := NewConnectionPool("localhost:5683", config, factory, logger, metrics)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		// Test basic operations
		ctx := context.Background()

		// First get should succeed (creates a connection)
		conn, err := pool.Get(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		// Verify connection properties
		assert.Equal(t, "localhost:5683", conn.endpoint)
		assert.Equal(t, int32(1), atomic.LoadInt32(&conn.inUse))
		assert.NotNil(t, conn.conn)

		// Return the connection
		pool.Put(conn)
		assert.Equal(t, int32(0), atomic.LoadInt32(&conn.inUse))

		// Get again - should reuse the same connection
		conn2, err := pool.Get(ctx)
		assert.NoError(t, err)
		assert.Equal(t, conn, conn2) // Should be the same connection

		pool.Put(conn2)
	})

	t.Run("Pool initialization", func(t *testing.T) {
		pool, err := NewConnectionPool("localhost:5684", config, factory, logger, metrics)
		require.NoError(t, err)
		require.NotNil(t, pool)

		// Check pool properties
		assert.Equal(t, "localhost:5684", pool.endpoint)
		assert.Equal(t, "udp", pool.protocol)
		assert.Equal(t, 3, pool.maxSize)
		assert.Equal(t, int32(0), pool.currentSize)
		assert.NotNil(t, pool.connections)
		assert.NotNil(t, pool.factory)
		assert.NotNil(t, pool.logger)
		assert.NotNil(t, pool.metrics)

		// Close should work without errors
		err = pool.Close()
		assert.NoError(t, err)
	})
}

func TestConnectionManagerWithBenthosMocks(t *testing.T) {
	config := Config{
		Endpoints:           []string{"localhost:5683", "localhost:5684"},
		Protocol:            "udp",
		MaxPoolSize:         5,
		IdleTimeout:         time.Minute,
		HealthCheckInterval: 10 * time.Second,
		ConnectTimeout:      5 * time.Second,
		Security: SecurityConfig{
			Mode: "none",
		},
	}

	// Use mock resources to get a proper logger
	resources := service.MockResources()
	logger := resources.Logger()

	t.Run("Create manager", func(t *testing.T) {
		manager, err := NewManager(config, logger, resources)
		require.NoError(t, err)
		require.NotNil(t, manager)

		assert.Equal(t, config, manager.config)
		assert.Len(t, manager.pools, 2)

		// Verify pools were created for each endpoint
		assert.Contains(t, manager.pools, "localhost:5683")
		assert.Contains(t, manager.pools, "localhost:5684")

		// Test that pools are properly initialized
		for endpoint, pool := range manager.pools {
			assert.NotNil(t, pool)
			assert.Equal(t, endpoint, pool.endpoint)
			assert.Equal(t, config.Protocol, pool.protocol)
			assert.Equal(t, config.MaxPoolSize, pool.maxSize)
		}

		// Close should work without errors
		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("Manager metrics initialization", func(t *testing.T) {
		manager, err := NewManager(config, logger, resources)
		require.NoError(t, err)
		require.NotNil(t, manager)
		defer manager.Close()

		// Verify metrics are properly initialized
		assert.NotNil(t, manager.metrics)
		assert.NotNil(t, manager.metrics.ConnectionsActive)
		assert.NotNil(t, manager.metrics.ConnectionsCreated)
		assert.NotNil(t, manager.metrics.ConnectionsFailed)
		assert.NotNil(t, manager.metrics.HealthChecksTotal)
		assert.NotNil(t, manager.metrics.HealthChecksFailed)
	})
}

func TestFactoryIntegration(t *testing.T) {
	t.Run("Create factories for all protocols", func(t *testing.T) {
		protocols := []string{"udp", "tcp", "udp-dtls", "tcp-tls"}

		for _, protocol := range protocols {
			factory, err := CreateFactory(protocol)
			require.NoError(t, err, "Failed to create factory for protocol: %s", protocol)
			require.NotNil(t, factory)
			assert.Equal(t, protocol, factory.Protocol())
		}
	})

	t.Run("Factory validates connection types", func(t *testing.T) {
		// Test that factories reject wrong connection types
		udpFactory := &UDPFactory{}
		tcpFactory := &TCPFactory{}

		// UDP factory should reject non-UDP connections
		err := udpFactory.Validate("not a connection")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")

		// TCP factory should reject non-TCP connections
		err = tcpFactory.Validate("not a connection")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})

	t.Run("Security config validation", func(t *testing.T) {
		// Test DTLS factory with valid PSK config
		dtlsFactory := &DTLSFactory{}
		validPSK := SecurityConfig{
			Mode:        "psk",
			PSKIdentity: "client1",
			PSKKey:      "secret",
		}

		dtlsConfig, err := dtlsFactory.createDTLSConfig(validPSK)
		assert.NoError(t, err)
		assert.NotNil(t, dtlsConfig)

		// Test TCP-TLS factory with invalid config
		tlsFactory := &TCPTLSFactory{}
		invalidCert := SecurityConfig{
			Mode:     "certificate",
			CertFile: "", // Missing cert file
			KeyFile:  "", // Missing key file
		}

		_, err = tlsFactory.createTLSConfig(invalidCert)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "certificate mode requires")
	})
}
