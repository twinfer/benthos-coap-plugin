// pkg/config/config.go
package config

import (
	"fmt"
	"strings"
	"time"
)

// CoAPConfig represents the complete configuration for CoAP input/output
type CoAPConfig struct {
	Endpoints      []string       `yaml:"endpoints"`
	ObservePaths   []string       `yaml:"observe_paths,omitempty"`
	DefaultPath    string         `yaml:"default_path,omitempty"`
	Protocol       string         `yaml:"protocol"`
	Security       SecurityConfig `yaml:"security"`
	ConnectionPool ConnectionPool `yaml:"connection_pool"`
	Observer       Observer       `yaml:"observer,omitempty"`
	RequestOptions RequestOptions `yaml:"request_options,omitempty"`
	RetryPolicy    RetryPolicy    `yaml:"retry_policy"`
	CircuitBreaker CircuitBreaker `yaml:"circuit_breaker"`
	Converter      Converter      `yaml:"converter"`
}

type SecurityConfig struct {
	Mode         string `yaml:"mode"`
	PSKIdentity  string `yaml:"psk_identity,omitempty"`
	PSKKey       string `yaml:"psk_key,omitempty"`
	CertFile     string `yaml:"cert_file,omitempty"`
	KeyFile      string `yaml:"key_file,omitempty"`
	CACertFile   string `yaml:"ca_cert_file,omitempty"`
	InsecureSkip bool   `yaml:"insecure_skip_verify"`
}

type ConnectionPool struct {
	MaxSize             int           `yaml:"max_size"`
	IdleTimeout         time.Duration `yaml:"idle_timeout"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	ConnectTimeout      time.Duration `yaml:"connect_timeout"`
}

type Observer struct {
	BufferSize       int           `yaml:"buffer_size"`
	ObserveTimeout   time.Duration `yaml:"observe_timeout"`
	ResubscribeDelay time.Duration `yaml:"resubscribe_delay"`
}

type RequestOptions struct {
	Confirmable      bool          `yaml:"confirmable"`
	Timeout          time.Duration `yaml:"timeout"`
	ContentFormat    string        `yaml:"content_format,omitempty"`
	AutoDetectFormat bool          `yaml:"auto_detect_format"`
}

type RetryPolicy struct {
	MaxRetries      int           `yaml:"max_retries"`
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	Multiplier      float64       `yaml:"multiplier"`
	Jitter          bool          `yaml:"jitter"`
}

type CircuitBreaker struct {
	Enabled          bool          `yaml:"enabled"`
	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`
	Timeout          time.Duration `yaml:"timeout"`
	HalfOpenMaxCalls int           `yaml:"half_open_max_calls"`
}

type Converter struct {
	DefaultContentFormat string `yaml:"default_content_format"`
	CompressionEnabled   bool   `yaml:"compression_enabled"`
	MaxPayloadSize       int    `yaml:"max_payload_size"`
	PreserveOptions      bool   `yaml:"preserve_options"`
}

// Validate performs comprehensive validation of the CoAP configuration
func (c *CoAPConfig) Validate() error {
	if err := c.validateEndpoints(); err != nil {
		return fmt.Errorf("invalid endpoints: %w", err)
	}

	if err := c.validateProtocol(); err != nil {
		return fmt.Errorf("invalid protocol: %w", err)
	}

	if err := c.validateSecurity(); err != nil {
		return fmt.Errorf("invalid security config: %w", err)
	}

	if err := c.validateObservePaths(); err != nil {
		return fmt.Errorf("invalid observe paths: %w", err)
	}

	if err := c.validateConnectionPool(); err != nil {
		return fmt.Errorf("invalid connection pool config: %w", err)
	}

	if err := c.validateRetryPolicy(); err != nil {
		return fmt.Errorf("invalid retry policy: %w", err)
	}

	if err := c.validateCircuitBreaker(); err != nil {
		return fmt.Errorf("invalid circuit breaker config: %w", err)
	}

	if err := c.validateConverter(); err != nil {
		return fmt.Errorf("invalid converter config: %w", err)
	}

	return nil
}

func (c *CoAPConfig) validateEndpoints() error {
	if len(c.Endpoints) == 0 {
		return fmt.Errorf("at least one endpoint is required")
	}

	for _, endpoint := range c.Endpoints {
		if err := ValidateEndpoint(endpoint, c.Protocol); err != nil {
			return fmt.Errorf("invalid endpoint %s: %w", endpoint, err)
		}
	}

	return nil
}

func (c *CoAPConfig) validateProtocol() error {
	return ValidateProtocol(c.Protocol)
}

func (c *CoAPConfig) validateSecurity() error {
	return ValidateSecurityConfig(c.Protocol, c.Security)
}

func (c *CoAPConfig) validateObservePaths() error {
	if len(c.ObservePaths) == 0 {
		return nil // Optional for output plugins
	}

	for _, path := range c.ObservePaths {
		if err := ValidateResourcePath(path); err != nil {
			return fmt.Errorf("invalid observe path %s: %w", path, err)
		}
	}

	return nil
}

func (c *CoAPConfig) validateConnectionPool() error {
	if c.ConnectionPool.MaxSize <= 0 {
		return fmt.Errorf("connection pool max_size must be positive")
	}

	if c.ConnectionPool.MaxSize > 100 {
		return fmt.Errorf("connection pool max_size too large (max: 100)")
	}

	if c.ConnectionPool.IdleTimeout < 0 {
		return fmt.Errorf("connection pool idle_timeout cannot be negative")
	}

	if c.ConnectionPool.ConnectTimeout <= 0 {
		return fmt.Errorf("connection pool connect_timeout must be positive")
	}

	return nil
}

func (c *CoAPConfig) validateRetryPolicy() error {
	if c.RetryPolicy.MaxRetries < 0 {
		return fmt.Errorf("retry policy max_retries cannot be negative")
	}

	if c.RetryPolicy.MaxRetries > 10 {
		return fmt.Errorf("retry policy max_retries too large (max: 10)")
	}

	if c.RetryPolicy.InitialInterval <= 0 {
		return fmt.Errorf("retry policy initial_interval must be positive")
	}

	if c.RetryPolicy.MaxInterval < c.RetryPolicy.InitialInterval {
		return fmt.Errorf("retry policy max_interval must be >= initial_interval")
	}

	if c.RetryPolicy.Multiplier <= 1.0 {
		return fmt.Errorf("retry policy multiplier must be > 1.0")
	}

	return nil
}

func (c *CoAPConfig) validateCircuitBreaker() error {
	if !c.CircuitBreaker.Enabled {
		return nil
	}

	if c.CircuitBreaker.FailureThreshold <= 0 {
		return fmt.Errorf("circuit breaker failure_threshold must be positive")
	}

	if c.CircuitBreaker.SuccessThreshold <= 0 {
		return fmt.Errorf("circuit breaker success_threshold must be positive")
	}

	if c.CircuitBreaker.Timeout <= 0 {
		return fmt.Errorf("circuit breaker timeout must be positive")
	}

	if c.CircuitBreaker.HalfOpenMaxCalls <= 0 {
		return fmt.Errorf("circuit breaker half_open_max_calls must be positive")
	}

	return nil
}

func (c *CoAPConfig) validateConverter() error {
	if c.Converter.MaxPayloadSize <= 0 {
		return fmt.Errorf("converter max_payload_size must be positive")
	}

	if c.Converter.MaxPayloadSize > 16*1024*1024 { // 16MB limit
		return fmt.Errorf("converter max_payload_size too large (max: 16MB)")
	}

	if c.Converter.DefaultContentFormat != "" {
		validFormats := []string{
			"text/plain", "application/json", "application/xml",
			"application/cbor", "application/octet-stream",
		}

		valid := false
		for _, format := range validFormats {
			if c.Converter.DefaultContentFormat == format {
				valid = true
				break
			}
		}

		if !valid {
			return fmt.Errorf("converter default_content_format must be one of: %s",
				strings.Join(validFormats, ", "))
		}
	}

	return nil
}

// ApplyDefaults sets default values for unspecified configuration options
func (c *CoAPConfig) ApplyDefaults() {
	if c.Protocol == "" {
		c.Protocol = "udp"
	}

	if c.Security.Mode == "" {
		c.Security.Mode = "none"
	}

	// Connection pool defaults
	if c.ConnectionPool.MaxSize == 0 {
		c.ConnectionPool.MaxSize = 5
	}
	if c.ConnectionPool.IdleTimeout == 0 {
		c.ConnectionPool.IdleTimeout = 30 * time.Second
	}
	if c.ConnectionPool.HealthCheckInterval == 0 {
		c.ConnectionPool.HealthCheckInterval = 10 * time.Second
	}
	if c.ConnectionPool.ConnectTimeout == 0 {
		c.ConnectionPool.ConnectTimeout = 10 * time.Second
	}

	// Observer defaults
	if c.Observer.BufferSize == 0 {
		c.Observer.BufferSize = 1000
	}
	if c.Observer.ObserveTimeout == 0 {
		c.Observer.ObserveTimeout = 5 * time.Minute
	}
	if c.Observer.ResubscribeDelay == 0 {
		c.Observer.ResubscribeDelay = 5 * time.Second
	}

	// Request options defaults
	if c.RequestOptions.Timeout == 0 {
		c.RequestOptions.Timeout = 30 * time.Second
	}
	if c.RequestOptions.ContentFormat == "" && !c.RequestOptions.AutoDetectFormat {
		c.RequestOptions.ContentFormat = "application/json"
		c.RequestOptions.AutoDetectFormat = true
	}

	// Retry policy defaults
	if c.RetryPolicy.MaxRetries == 0 {
		c.RetryPolicy.MaxRetries = 3
	}
	if c.RetryPolicy.InitialInterval == 0 {
		c.RetryPolicy.InitialInterval = 1 * time.Second
	}
	if c.RetryPolicy.MaxInterval == 0 {
		c.RetryPolicy.MaxInterval = 30 * time.Second
	}
	if c.RetryPolicy.Multiplier == 0 {
		c.RetryPolicy.Multiplier = 2.0
	}

	// Circuit breaker defaults
	if c.CircuitBreaker.FailureThreshold == 0 {
		c.CircuitBreaker.FailureThreshold = 5
	}
	if c.CircuitBreaker.SuccessThreshold == 0 {
		c.CircuitBreaker.SuccessThreshold = 3
	}
	if c.CircuitBreaker.Timeout == 0 {
		c.CircuitBreaker.Timeout = 30 * time.Second
	}
	if c.CircuitBreaker.HalfOpenMaxCalls == 0 {
		c.CircuitBreaker.HalfOpenMaxCalls = 2
	}

	// Converter defaults
	if c.Converter.DefaultContentFormat == "" {
		c.Converter.DefaultContentFormat = "application/json"
	}
	if c.Converter.MaxPayloadSize == 0 {
		c.Converter.MaxPayloadSize = 1024 * 1024 // 1MB
	}
}

// Clone creates a deep copy of the configuration
func (c *CoAPConfig) Clone() *CoAPConfig {
	clone := *c

	// Deep copy slices
	clone.Endpoints = make([]string, len(c.Endpoints))
	copy(clone.Endpoints, c.Endpoints)

	clone.ObservePaths = make([]string, len(c.ObservePaths))
	copy(clone.ObservePaths, c.ObservePaths)

	return &clone
}
