// pkg/config/defaults.go
package config

import "time"

// DefaultInputConfig returns a default configuration for CoAP input
func DefaultInputConfig() *CoAPConfig {
	config := &CoAPConfig{
		Protocol: "udp",
		Security: SecurityConfig{
			Mode: "none",
		},
		ConnectionPool: ConnectionPool{
			MaxSize:             5,
			IdleTimeout:         30 * time.Second,
			HealthCheckInterval: 10 * time.Second,
			ConnectTimeout:      10 * time.Second,
		},
		Observer: Observer{
			BufferSize:       1000,
			ObserveTimeout:   5 * time.Minute,
			ResubscribeDelay: 5 * time.Second,
		},
		RetryPolicy: RetryPolicy{
			MaxRetries:      3,
			InitialInterval: 1 * time.Second,
			MaxInterval:     30 * time.Second,
			Multiplier:      2.0,
			Jitter:          true,
		},
		CircuitBreaker: CircuitBreaker{
			Enabled:          true,
			FailureThreshold: 5,
			SuccessThreshold: 3,
			Timeout:          30 * time.Second,
			HalfOpenMaxCalls: 2,
		},
		Converter: Converter{
			DefaultContentFormat: "application/json",
			CompressionEnabled:   true,
			MaxPayloadSize:       1024 * 1024, // 1MB
			PreserveOptions:      false,
		},
	}

	return config
}

// DefaultOutputConfig returns a default configuration for CoAP output
func DefaultOutputConfig() *CoAPConfig {
	config := DefaultInputConfig()

	// Output-specific defaults
	config.DefaultPath = "/"
	config.RequestOptions = RequestOptions{
		Confirmable:      true,
		Timeout:          30 * time.Second,
		ContentFormat:    "application/json",
		AutoDetectFormat: true,
	}

	// Different retry policy for output
	config.RetryPolicy = RetryPolicy{
		MaxRetries:      3,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      1.5,
		Jitter:          true,
	}

	return config
}

// HighThroughputConfig returns a configuration optimized for high throughput
func HighThroughputConfig() *CoAPConfig {
	config := DefaultInputConfig()

	config.Protocol = "tcp" // TCP for higher throughput
	config.ConnectionPool.MaxSize = 20
	config.Observer.BufferSize = 50000
	config.Converter.CompressionEnabled = true
	config.Converter.MaxPayloadSize = 2 * 1024 * 1024 // 2MB

	return config
}

// LowLatencyConfig returns a configuration optimized for low latency
func LowLatencyConfig() *CoAPConfig {
	config := DefaultInputConfig()

	config.Protocol = "udp" // UDP for lowest latency
	config.Observer.BufferSize = 10
	config.ConnectionPool.HealthCheckInterval = 1 * time.Second
	config.RequestOptions.Confirmable = false // Non-confirmable for speed

	return config
}

// SecureConfig returns a configuration with security enabled
func SecureConfig() *CoAPConfig {
	config := DefaultInputConfig()

	config.Protocol = "udp-dtls"
	config.Security = SecurityConfig{
		Mode: "psk",
		// PSKIdentity and PSKKey should be set by user
	}

	return config
}
