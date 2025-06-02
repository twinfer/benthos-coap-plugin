// pkg/input/coap.go
package input

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
	"github.com/twinfer/benthos-coap-plugin/pkg/observer"
)

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Reads messages from CoAP endpoints using observe subscriptions.").
		Description("The CoAP input establishes observe subscriptions on specified paths and converts incoming CoAP messages to Benthos messages. Supports UDP, TCP, DTLS, and TCP-TLS protocols with automatic reconnection and circuit breaker patterns.").
		Field(service.NewStringListField("endpoints").
			Description("List of CoAP endpoints to connect to.").
			Example([]string{"coap://localhost:5683", "coaps://device.local:5684"})).
		Field(service.NewStringListField("observe_paths").
			Description("List of resource paths to observe for real-time updates.").
			Example([]string{"/sensors/temperature", "/actuators/+/status"})).
		Field(service.NewStringField("protocol").
			Description("CoAP protocol to use.").
			Default("udp").
			LintRule("root in ['udp', 'tcp', 'udp-dtls', 'tcp-tls']")).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").
				Description("Security mode: none, psk, or certificate.").
				Default("none").
				LintRule("root in ['none', 'psk', 'certificate']"),
			service.NewStringField("psk_identity").
				Description("PSK identity for DTLS authentication.").
				Optional(),
			service.NewStringField("psk_key").
				Description("PSK key for DTLS authentication.").
				Optional(),
			service.NewStringField("cert_file").
				Description("Path to client certificate file.").
				Optional(),
			service.NewStringField("key_file").
				Description("Path to client private key file.").
				Optional(),
			service.NewStringField("ca_cert_file").
				Description("Path to CA certificate file for verification.").
				Optional(),
			service.NewBoolField("insecure_skip_verify").
				Description("Skip certificate verification (insecure).").
				Default(false),
		).Description("Security configuration for DTLS/TLS connections.")).
		Field(service.NewObjectField("connection_pool",
			service.NewIntField("max_size").
				Description("Maximum number of connections per endpoint.").
				Default(5),
			service.NewDurationField("idle_timeout").
				Description("How long to keep idle connections open.").
				Default("30s"),
			service.NewDurationField("health_check_interval").
				Description("Interval for connection health checks.").
				Default("10s"),
			service.NewDurationField("connect_timeout").
				Description("Timeout for establishing new connections.").
				Default("10s"),
		).Description("Connection pool configuration.")).
		Field(service.NewObjectField("observer",
			service.NewIntField("buffer_size").
				Description("Size of the message buffer for observe notifications.").
				Default(1000),
			service.NewDurationField("observe_timeout").
				Description("Timeout for individual observe operations.").
				Default("5m"),
			service.NewDurationField("resubscribe_delay").
				Description("Delay before resubscribing after failure.").
				Default("5s"),
		).Description("Observer configuration for CoAP observe subscriptions.")).
		Field(service.NewObjectField("retry_policy",
			service.NewIntField("max_retries").
				Description("Maximum number of retry attempts.").
				Default(3),
			service.NewDurationField("initial_interval").
				Description("Initial retry interval.").
				Default("1s"),
			service.NewDurationField("max_interval").
				Description("Maximum retry interval.").
				Default("30s"),
			service.NewFloatField("multiplier").
				Description("Retry interval multiplier.").
				Default(2.0),
			service.NewBoolField("jitter").
				Description("Add random jitter to retry intervals.").
				Default(true),
		).Description("Retry policy for failed operations.")).
		Field(service.NewObjectField("circuit_breaker",
			service.NewBoolField("enabled").
				Description("Enable circuit breaker pattern.").
				Default(true),
			service.NewIntField("failure_threshold").
				Description("Number of failures before opening circuit.").
				Default(5),
			service.NewIntField("success_threshold").
				Description("Number of successes required to close circuit.").
				Default(3),
			service.NewDurationField("timeout").
				Description("How long to wait before attempting to close circuit.").
				Default("30s"),
			service.NewIntField("half_open_max_calls").
				Description("Maximum calls allowed in half-open state.").
				Default(2),
		).Description("Circuit breaker configuration.")).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").
				Description("Default content format for messages without explicit format.").
				Default("application/json"),
			service.NewBoolField("compression_enabled").
				Description("Enable payload compression for large messages.").
				Default(true),
			service.NewIntField("max_payload_size").
				Description("Maximum payload size in bytes.").
				Default(1048576),
			service.NewBoolField("preserve_options").
				Description("Preserve all CoAP options in message metadata.").
				Default(false),
		).Description("Message conversion configuration."))

	err := service.RegisterInput("coap", configSpec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newCoAPInput(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type Input struct {
	connManager *connection.Manager
	obsManager  *observer.Manager
	converter   *converter.Converter
	logger      *service.Logger
	metrics     *Metrics

	msgChan   chan *service.Message
	closeChan chan struct{}
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
}

type Metrics struct {
	MessagesRead    *service.MetricCounter
	MessagesDropped *service.MetricCounter
	ConnectionsOpen *service.MetricCounter
	ObservesActive  *service.MetricCounter
	ErrorsTotal     *service.MetricCounter
}

func newCoAPInput(conf *service.ParsedConfig, mgr *service.Resources) (*Input, error) {
	// Parse configuration
	endpoints, err := conf.FieldStringList("endpoints")
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoints: %w", err)
	}

	observePaths, err := conf.FieldStringList("observe_paths")
	if err != nil {
		return nil, fmt.Errorf("failed to parse observe_paths: %w", err)
	}

	protocol, err := conf.FieldString("protocol")
	if err != nil {
		return nil, fmt.Errorf("failed to parse protocol: %w", err)
	}

	// Parse security config
	securityConf, err := conf.FieldObjectMap("security")
	if err != nil {
		return nil, fmt.Errorf("failed to parse security config: %w", err)
	}

	securityMode := securityConf["mode"].(string)
	security := connection.SecurityConfig{
		Mode: securityMode,
	}

	if securityMode == "psk" {
		if identity, exists := securityConf["psk_identity"]; exists {
			security.PSKIdentity = identity.(string)
		}
		if key, exists := securityConf["psk_key"]; exists {
			security.PSKKey = key.(string)
		}
	} else if securityMode == "certificate" {
		if certFile, exists := securityConf["cert_file"]; exists {
			security.CertFile = certFile.(string)
		}
		if keyFile, exists := securityConf["key_file"]; exists {
			security.KeyFile = keyFile.(string)
		}
		if caCertFile, exists := securityConf["ca_cert_file"]; exists {
			security.CACertFile = caCertFile.(string)
		}
		if insecureSkip, exists := securityConf["insecure_skip_verify"]; exists {
			security.InsecureSkip = insecureSkip.(bool)
		}
	}

	// Parse connection pool config
	poolConf, err := conf.FieldObjectMap("connection_pool")
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection_pool config: %w", err)
	}

	maxSize := poolConf["max_size"].(int)
	idleTimeout := poolConf["idle_timeout"].(time.Duration)
	healthCheckInterval := poolConf["health_check_interval"].(time.Duration)
	connectTimeout := poolConf["connect_timeout"].(time.Duration)

	// Parse observer config
	obsConf, err := conf.FieldObjectMap("observer")
	if err != nil {
		return nil, fmt.Errorf("failed to parse observer config: %w", err)
	}

	bufferSize := obsConf["buffer_size"].(int)
	observeTimeout := obsConf["observe_timeout"].(time.Duration)
	resubscribeDelay := obsConf["resubscribe_delay"].(time.Duration)

	// Parse retry policy
	retryConf, err := conf.FieldObjectMap("retry_policy")
	if err != nil {
		return nil, fmt.Errorf("failed to parse retry_policy config: %w", err)
	}

	retryPolicy := observer.RetryPolicy{
		MaxRetries:      retryConf["max_retries"].(int),
		InitialInterval: retryConf["initial_interval"].(time.Duration),
		MaxInterval:     retryConf["max_interval"].(time.Duration),
		Multiplier:      retryConf["multiplier"].(float64),
		Jitter:          retryConf["jitter"].(bool),
	}

	// Parse circuit breaker config
	cbConf, err := conf.FieldObjectMap("circuit_breaker")
	if err != nil {
		return nil, fmt.Errorf("failed to parse circuit_breaker config: %w", err)
	}

	circuitConfig := observer.CircuitConfig{
		Enabled:          cbConf["enabled"].(bool),
		FailureThreshold: cbConf["failure_threshold"].(int),
		SuccessThreshold: cbConf["success_threshold"].(int),
		Timeout:          cbConf["timeout"].(time.Duration),
		HalfOpenMaxCalls: cbConf["half_open_max_calls"].(int),
	}

	// Parse converter config
	convConf, err := conf.FieldObjectMap("converter")
	if err != nil {
		return nil, fmt.Errorf("failed to parse converter config: %w", err)
	}

	converterConfig := converter.Config{
		DefaultContentFormat: convConf["default_content_format"].(string),
		CompressionEnabled:   convConf["compression_enabled"].(bool),
		MaxPayloadSize:       convConf["max_payload_size"].(int),
		PreserveOptions:      convConf["preserve_options"].(bool),
	}

	// Initialize components
	connConfig := connection.Config{
		Endpoints:           endpoints,
		Protocol:            protocol,
		MaxPoolSize:         maxSize,
		IdleTimeout:         idleTimeout,
		HealthCheckInterval: healthCheckInterval,
		ConnectTimeout:      connectTimeout,
		Security:            security,
	}

	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	conv := converter.NewConverter(converterConfig, mgr.Logger())

	obsConfig := observer.Config{
		ObservePaths:     observePaths,
		RetryPolicy:      retryPolicy,
		CircuitBreaker:   circuitConfig,
		BufferSize:       bufferSize,
		ObserveTimeout:   observeTimeout,
		ResubscribeDelay: resubscribeDelay,
	}

	obsManager, err := observer.NewManager(obsConfig, connManager, conv, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create observer manager: %w", err)
	}

	input := &Input{
		connManager: connManager,
		obsManager:  obsManager,
		converter:   conv,
		logger:      mgr.Logger(),
		msgChan:     make(chan *service.Message, bufferSize),
		closeChan:   make(chan struct{}),
		metrics: &Metrics{
			MessagesRead:    mgr.MetricCounter("coap_input_messages_read"),
			MessagesDropped: mgr.MetricCounter("coap_input_messages_dropped"),
			ConnectionsOpen: mgr.MetricCounter("coap_input_connections_open"),
			ObservesActive:  mgr.MetricCounter("coap_input_observes_active"),
			ErrorsTotal:     mgr.MetricCounter("coap_input_errors_total"),
		},
	}

	return input, nil
}

func (i *Input) Connect(ctx context.Context) error {
	i.mu.RLock()
	if i.closed {
		i.mu.RUnlock()
		return service.ErrEndOfInput
	}
	i.mu.RUnlock()

	i.logger.Info(fmt.Sprintf("Starting CoAP input with endpoints: %v", i.connManager.Config().Endpoints))

	// Start observer manager
	if err := i.obsManager.Start(); err != nil {
		return fmt.Errorf("failed to start observer manager: %w", err)
	}

	// Start message forwarding goroutine
	go i.forwardMessages()

	i.logger.Info("CoAP input connected successfully")
	return nil
}

func (i *Input) forwardMessages() {
	defer func() {
		if r := recover(); r != nil {
			i.logger.Error(fmt.Sprintf("Message forwarding panic: %v", r))
		}
	}()

	for {
		select {
		case msg, ok := <-i.obsManager.MessageChan():
			if !ok {
				i.logger.Debug("Observer message channel closed")
				return
			}

			select {
			case i.msgChan <- msg:
				i.metrics.MessagesRead.Incr(1)
			default:
				i.logger.Warn("Message buffer full, dropping message")
				i.metrics.MessagesDropped.Incr(1)
			}

		case <-i.closeChan:
			return
		}
	}
}

func (i *Input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	i.mu.RLock()
	closed := i.closed
	i.mu.RUnlock()

	if closed {
		return nil, nil, service.ErrEndOfInput
	}

	select {
	case msg := <-i.msgChan:
		return msg, func(ctx context.Context, err error) error {
			if err != nil {
				i.logger.Error(fmt.Sprintf("Message processing failed: %v", err))
				i.metrics.ErrorsTotal.Incr(1)
			}
			return nil
		}, nil

	case <-ctx.Done():
		return nil, nil, ctx.Err()

	case <-i.closeChan:
		return nil, nil, service.ErrEndOfInput
	}
}

func (i *Input) Close(ctx context.Context) error {
	i.closeOnce.Do(func() {
		i.mu.Lock()
		i.closed = true
		i.mu.Unlock()

		close(i.closeChan)

		i.logger.Info("Closing CoAP input")

		// Close observer manager
		if err := i.obsManager.Close(); err != nil {
			i.logger.Error(fmt.Sprintf("Failed to close observer manager: %v", err))
		}

		// Close connection manager
		if err := i.connManager.Close(); err != nil {
			i.logger.Error(fmt.Sprintf("Failed to close connection  manager: %v", err))
		}

		// Drain remaining messages
		go func() {
			for range i.msgChan {
				// Drain channel
			}
		}()

		i.logger.Info("CoAP input closed")
	})

	return nil
}

// Health returns the current health status of the input
func (i *Input) Health() map[string]interface{} {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.closed {
		return map[string]interface{}{
			"status": "closed",
		}
	}

	status := map[string]interface{}{
		"status":       "healthy",
		"buffer_usage": fmt.Sprintf("%d/%d", len(i.msgChan), cap(i.msgChan)),
	}

	if i.obsManager != nil {
		status["observers"] = i.obsManager.HealthStatus()
	}

	return status
}
