package connection

import (
	"context"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripScheme(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		expected string
	}{
		{
			name:     "coap scheme",
			endpoint: "coap://localhost:5683",
			expected: "localhost:5683",
		},
		{
			name:     "coaps scheme",
			endpoint: "coaps://localhost:5684",
			expected: "localhost:5684",
		},
		{
			name:     "no scheme",
			endpoint: "localhost:5683",
			expected: "localhost:5683",
		},
		{
			name:     "ip address with coap",
			endpoint: "coap://127.0.0.1:5683",
			expected: "127.0.0.1:5683",
		},
		{
			name:     "ip address without scheme",
			endpoint: "127.0.0.1:5683",
			expected: "127.0.0.1:5683",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripScheme(tt.endpoint)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestManagerEndpointHandling(t *testing.T) {
	mgr := service.MockResources()

	// Test with scheme in endpoint
	config := Config{
		Endpoints:   []string{"coap://localhost:5683"},
		Protocol:    "udp",
		MaxPoolSize: 1,
	}

	manager, err := NewManager(config, mgr.Logger(), mgr)
	require.NoError(t, err)

	// Check that the manager has the expected mappings
	assert.Len(t, manager.pools, 1)
	assert.Len(t, manager.endpointMap, 1)

	// The pool should be keyed by the original endpoint
	_, exists := manager.pools["coap://localhost:5683"]
	assert.True(t, exists)

	// The endpoint map should map cleaned endpoint to original
	originalEndpoint, mapped := manager.endpointMap["localhost:5683"]
	assert.True(t, mapped)
	assert.Equal(t, "coap://localhost:5683", originalEndpoint)
}

func TestManagerWithMultipleEndpoints(t *testing.T) {
	mgr := service.MockResources()

	config := Config{
		Endpoints: []string{
			"coap://localhost:5683",
			"localhost:5684",
			"coaps://example.com:5685",
		},
		Protocol:    "udp",
		MaxPoolSize: 1,
	}

	manager, err := NewManager(config, mgr.Logger(), mgr)
	require.NoError(t, err)

	// Should have 3 pools
	assert.Len(t, manager.pools, 3)
	assert.Len(t, manager.endpointMap, 3)

	// Check that all endpoints are properly mapped
	expectedMappings := map[string]string{
		"localhost:5683":   "coap://localhost:5683",
		"localhost:5684":   "localhost:5684",
		"example.com:5685": "coaps://example.com:5685",
	}

	for cleanEndpoint, originalEndpoint := range expectedMappings {
		mapped, exists := manager.endpointMap[cleanEndpoint]
		assert.True(t, exists, "Missing mapping for %s", cleanEndpoint)
		assert.Equal(t, originalEndpoint, mapped)

		_, poolExists := manager.pools[originalEndpoint]
		assert.True(t, poolExists, "Missing pool for %s", originalEndpoint)
	}
}

func TestManagerGetConnectionSuccess(t *testing.T) {
	mgr := service.MockResources()

	// UDP connections don't fail immediately for non-existent endpoints
	// so let's test successful connection creation instead
	config := Config{
		Endpoints:      []string{"coap://localhost:9999"},
		Protocol:       "udp",
		MaxPoolSize:    1,
		ConnectTimeout: 100 * time.Millisecond,
	}

	manager, err := NewManager(config, mgr.Logger(), mgr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// UDP connection should succeed (UDP is connectionless)
	conn, err := manager.Get(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, conn)

	// Clean up
	if conn != nil {
		manager.Put(conn)
	}
}
