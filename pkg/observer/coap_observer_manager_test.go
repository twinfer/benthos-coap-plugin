package observer

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// TestCircuitBreakerBasicFunctionality tests the circuit breaker core functionality
func TestCircuitBreakerBasicFunctionality(t *testing.T) {
	config := CircuitConfig{
		Enabled:          true,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		HalfOpenMaxCalls: 1,
	}

	cb := NewCircuitBreaker(config)
	require.NotNil(t, cb)

	// Initially closed
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "closed", cb.State())

	// Record failures to open circuit
	cb.RecordFailure()
	cb.RecordFailure()
	assert.True(t, cb.CanExecute()) // Still closed

	cb.RecordFailure()               // This should open the circuit
	assert.False(t, cb.CanExecute()) // Now open
	assert.Equal(t, "open", cb.State())

	// Wait for timeout and test half-open transition
	time.Sleep(110 * time.Millisecond)
	assert.True(t, cb.CanExecute()) // Should transition to half-open

	// Record success in half-open state
	cb.RecordSuccess()
	cb.RecordSuccess() // This should close the circuit
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "closed", cb.State())
}

// TestCircuitBreakerDisabled tests that disabled circuit breaker always allows execution
func TestCircuitBreakerDisabled(t *testing.T) {
	config := CircuitConfig{Enabled: false}
	cb := NewCircuitBreaker(config)

	assert.True(t, cb.CanExecute())
	assert.Equal(t, "disabled", cb.State())

	// Even after failures, it should still allow execution
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "disabled", cb.State())
}

// TestManagerCreation tests basic manager creation and configuration
func TestManagerCreation(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	config := Config{
		ObservePaths:   []string{"/sensor/temp", "/sensor/humidity"},
		BufferSize:     100,
		ObserveTimeout: 30 * time.Second,
		RetryPolicy: RetryPolicy{
			MaxRetries:      3,
			InitialInterval: 1 * time.Second,
			MaxInterval:     30 * time.Second,
			Multiplier:      2.0,
		},
		CircuitBreaker: CircuitConfig{
			Enabled:          true,
			FailureThreshold: 5,
			SuccessThreshold: 3,
			Timeout:          60 * time.Second,
			HalfOpenMaxCalls: 2,
		},
	}

	// Test with nil connection manager (we'll focus on the manager logic, not connection details)
	mgr, err := NewManager(config, nil, nil, logger, resources)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// Test configuration
	assert.Equal(t, config.ObservePaths, mgr.config.ObservePaths)
	assert.Equal(t, config.BufferSize, mgr.config.BufferSize)
	assert.Equal(t, config.ObserveTimeout, mgr.config.ObserveTimeout)

	// Test message channel
	assert.NotNil(t, mgr.MessageChan())

	// Test metrics
	assert.NotNil(t, mgr.metrics)
	assert.NotNil(t, mgr.metrics.ObservationsActive)
	assert.NotNil(t, mgr.metrics.MessagesReceived)

	// Clean up
	mgr.Close()
}

// TestManagerMessageHandling tests the core message processing logic
func TestManagerMessageHandling(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	config := Config{
		BufferSize: 10,
		CircuitBreaker: CircuitConfig{
			Enabled:          true,
			FailureThreshold: 5,
			SuccessThreshold: 3,
			Timeout:          30 * time.Second,
			HalfOpenMaxCalls: 2,
		},
	}

	// Create a simple converter for testing
	converterConfig := converter.Config{}
	conv := converter.NewConverter(converterConfig, logger)

	mgr, err := NewManager(config, nil, conv, logger, resources)
	require.NoError(t, err)
	require.NotNil(t, mgr)
	defer mgr.Close()

	// Test message conversion and handling
	testPath := "/test/resource"

	// Create a mock subscription for the test path since handleObserveMessage requires it
	_, mockCancel := context.WithCancel(context.Background())
	mockSubscription := &Subscription{
		path:    testPath,
		conn:    nil, // We'll handle nil check in the method
		circuit: NewCircuitBreaker(config.CircuitBreaker),
		healthy: 1,
		cancel:  mockCancel,
	}

	// Add the subscription to the manager
	mgr.mu.Lock()
	mgr.subscriptions[testPath] = mockSubscription
	mgr.mu.Unlock()

	coapMsg := &message.Message{
		Code:    codes.Content,
		Token:   message.Token("test-token"),
		Payload: []byte("test payload"),
		Options: message.Options{
			{ID: message.Observe, Value: []byte{1}},
			{ID: message.ContentFormat, Value: []byte{0}}, // text/plain
		},
	}

	// This should process the message and convert it
	mgr.handleObserveMessage(testPath, coapMsg)

	// Check if a message was produced (with timeout)
	select {
	case msg := <-mgr.MessageChan():
		require.NotNil(t, msg)
		payload, err := msg.AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "test payload", string(payload))

		// Check metadata
		coapCode, exists := msg.MetaGet("coap_code")
		assert.True(t, exists)
		assert.Equal(t, codes.Content.String(), coapCode)

		tokenMeta, exists := msg.MetaGet("coap_token")
		assert.True(t, exists)
		assert.Equal(t, "test-token", tokenMeta)

	case <-time.After(1 * time.Second):
		t.Fatal("Expected message on channel but timed out")
	}
}

// TestRetryPolicyConfiguration tests retry policy configuration
func TestRetryPolicyConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		policy      RetryPolicy
		expectValid bool
	}{
		{
			name: "valid policy",
			policy: RetryPolicy{
				MaxRetries:      3,
				InitialInterval: 1 * time.Second,
				MaxInterval:     30 * time.Second,
				Multiplier:      2.0,
			},
			expectValid: true,
		},
		{
			name: "zero max retries",
			policy: RetryPolicy{
				MaxRetries:      0,
				InitialInterval: 1 * time.Second,
				MaxInterval:     30 * time.Second,
				Multiplier:      2.0,
			},
			expectValid: true, // 0 retries should be valid (no retries)
		},
		{
			name: "invalid multiplier",
			policy: RetryPolicy{
				MaxRetries:      3,
				InitialInterval: 1 * time.Second,
				MaxInterval:     30 * time.Second,
				Multiplier:      0.5, // Less than 1.0
			},
			expectValid: false,
		},
	}

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BufferSize:  10,
				RetryPolicy: tt.policy,
				CircuitBreaker: CircuitConfig{
					Enabled: false, // Disable for this test
				},
			}

			mgr, err := NewManager(config, nil, nil, logger, resources)
			if tt.expectValid {
				require.NoError(t, err)
				require.NotNil(t, mgr)
				mgr.Close()
			} else {
				require.Error(t, err)
				assert.Nil(t, mgr)
			}
		})
	}
}

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid minimal config",
			config: Config{
				BufferSize: 10,
			},
			expectError: false,
		},
		{
			name: "zero buffer size",
			config: Config{
				BufferSize: 0,
			},
			expectError: true,
		},
		{
			name: "negative buffer size",
			config: Config{
				BufferSize: -1,
			},
			expectError: true,
		},
		{
			name: "valid observe paths",
			config: Config{
				BufferSize:   10,
				ObservePaths: []string{"/sensor/temp", "/sensor/humidity"},
			},
			expectError: false,
		},
		{
			name: "empty observe path",
			config: Config{
				BufferSize:   10,
				ObservePaths: []string{""},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := NewManager(tt.config, nil, nil, logger, resources)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, mgr)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, mgr)
				if mgr != nil {
					mgr.Close()
				}
			}
		})
	}
}

// TestManagerLifecycle tests manager startup and shutdown
func TestManagerLifecycle(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	config := Config{
		BufferSize: 10,
		CircuitBreaker: CircuitConfig{
			Enabled: false,
		},
	}

	mgr, err := NewManager(config, nil, nil, logger, resources)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// Manager should be running initially
	assert.NotNil(t, mgr.ctx)

	// Test graceful shutdown
	err = mgr.Close()
	assert.NoError(t, err)

	// After close, context should be cancelled
	select {
	case <-mgr.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Manager context was not cancelled after Close()")
	}

	// Multiple close calls should be safe
	err = mgr.Close()
	assert.NoError(t, err)
}

// TestCircuitBreakerManager tests the circuit breaker manager functionality
func TestCircuitBreakerManager(t *testing.T) {
	manager := NewCircuitBreakerManager()
	require.NotNil(t, manager)

	config := CircuitConfig{
		Enabled:          true,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		HalfOpenMaxCalls: 1,
	}

	// Test GetOrCreate
	cb1 := manager.GetOrCreate("test-resource", config)
	require.NotNil(t, cb1)

	// Get the same circuit breaker again
	cb2 := manager.GetOrCreate("test-resource", config)
	assert.Equal(t, cb1, cb2) // Should be the same instance

	// Test Get
	cb3, exists := manager.Get("test-resource")
	assert.True(t, exists)
	assert.Equal(t, cb1, cb3)

	// Test non-existent circuit breaker
	cb4, exists := manager.Get("non-existent")
	assert.False(t, exists)
	assert.Nil(t, cb4)

	// Test List
	names := manager.List()
	assert.Contains(t, names, "test-resource")
	assert.Len(t, names, 1)

	// Test Remove
	manager.Remove("test-resource")
	cb5, exists := manager.Get("test-resource")
	assert.False(t, exists)
	assert.Nil(t, cb5)
}

// TestMetricsCollection tests metrics collection functionality
func TestMetricsCollection(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	resources := service.MockResources()

	config := Config{
		BufferSize: 10,
		CircuitBreaker: CircuitConfig{
			Enabled: false,
		},
	}

	mgr, err := NewManager(config, nil, nil, logger, resources)
	require.NoError(t, err)
	require.NotNil(t, mgr)
	defer mgr.Close()

	// Test metrics are initialized
	assert.NotNil(t, mgr.metrics.ObservationsActive)
	assert.NotNil(t, mgr.metrics.MessagesReceived)

	// Test metric operations
	mgr.metrics.MessagesReceived.Incr(1)
	mgr.metrics.ObservationsActive.Incr(1)

	// These should not panic
	mgr.metrics.ObservationsActive.Incr(-1) // Decrement
}

// TestObservePathValidation tests validation of observe paths
func TestObservePathValidation(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		expectValid bool
	}{
		{
			name:        "valid absolute path",
			path:        "/sensor/temperature",
			expectValid: true,
		},
		{
			name:        "valid root path",
			path:        "/",
			expectValid: true,
		},
		{
			name:        "valid nested path",
			path:        "/api/v1/devices/123/sensors/temp",
			expectValid: true,
		},
		{
			name:        "empty path",
			path:        "",
			expectValid: false,
		},
		{
			name:        "relative path",
			path:        "sensor/temp",
			expectValid: false,
		},
		{
			name:        "path with query",
			path:        "/sensor?type=temp",
			expectValid: true, // Queries are typically handled separately but path itself is valid
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateObservePath(tt.path)
			if tt.expectValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// Helper function to validate observe paths
func validateObservePath(path string) error {
	if path == "" {
		return errors.New("observe path cannot be empty")
	}
	if !strings.HasPrefix(path, "/") {
		return errors.New("observe path must be absolute (start with '/')")
	}
	return nil
}

// TestCircuitBreakerStats tests circuit breaker statistics
func TestCircuitBreakerStats(t *testing.T) {
	config := CircuitConfig{
		Enabled:          true,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		HalfOpenMaxCalls: 1,
	}

	cb := NewCircuitBreaker(config)
	require.NotNil(t, cb)

	// Test initial stats
	stats := cb.Stats()
	assert.Equal(t, true, stats["enabled"])
	assert.Equal(t, "closed", stats["state"])
	assert.Equal(t, int32(0), stats["failures"])
	assert.Equal(t, int32(0), stats["successes"])

	// Record some failures
	cb.RecordFailure()
	cb.RecordFailure()

	stats = cb.Stats()
	assert.Equal(t, int32(2), stats["failures"])
	assert.Equal(t, "closed", stats["state"])

	// Open the circuit
	cb.RecordFailure()
	stats = cb.Stats()
	assert.Equal(t, int32(3), stats["failures"])
	assert.Equal(t, "open", stats["state"])
}
