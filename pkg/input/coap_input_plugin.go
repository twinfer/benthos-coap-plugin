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
	wg        sync.WaitGroup
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
	security, err := parseSecurityConfig(conf)
	if err != nil {
		return nil, err // Error already has context from parseSecurityConfig
	}

	// Parse connection config (includes pool settings)
	connConfig, err := parseConnectionConfig(conf, endpoints, protocol, security)
	if err != nil {
		return nil, err
	}

	// Parse observer config (includes retry and circuit breaker)
	obsConfig, err := parseObserverConfig(conf, observePaths)
	if err != nil {
		return nil, err
	}

	// Parse converter config
	converterConfig, err := parseConverterConfig(conf)
	if err != nil {
		return nil, err
	}

	// Initialize components
	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager for endpoints %v: %w", connConfig.Endpoints, err)
	}

	conv := converter.NewConverter(converterConfig, mgr.Logger())

	obsManager, err := observer.NewManager(obsConfig, connManager, conv, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create observer manager for paths %v: %w", obsConfig.ObservePaths, err)
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

func parseSecurityConfig(conf *service.ParsedConfig) (connection.SecurityConfig, error) {
	securityMode, err := conf.FieldString("security", "mode")
	if err != nil {
		return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.mode: %w", err)
	}
	security := connection.SecurityConfig{
		Mode: securityMode,
	}

	if securityMode == "psk" {
		if conf.ContainsPath("security", "psk_identity") {
			security.PSKIdentity, err = conf.FieldString("security", "psk_identity")
			if err != nil {
				return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.psk_identity: %w", err)
			}
		}
		if conf.ContainsPath("security", "psk_key") {
			security.PSKKey, err = conf.FieldString("security", "psk_key")
			if err != nil {
				return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.psk_key: %w", err)
			}
		}
	} else if securityMode == "certificate" {
		if conf.ContainsPath("security", "cert_file") {
			security.CertFile, err = conf.FieldString("security", "cert_file")
			if err != nil {
				return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.cert_file: %w", err)
			}
		}
		if conf.ContainsPath("security", "key_file") {
			security.KeyFile, err = conf.FieldString("security", "key_file")
			if err != nil {
				return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.key_file: %w", err)
			}
		}
		if conf.ContainsPath("security", "ca_cert_file") {
			security.CACertFile, err = conf.FieldString("security", "ca_cert_file")
			if err != nil {
				return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.ca_cert_file: %w", err)
			}
		}
		security.InsecureSkip, err = conf.FieldBool("security", "insecure_skip_verify")
		if err != nil {
			return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.insecure_skip_verify: %w", err)
		}
	}
	return security, nil
}

func parseConnectionConfig(conf *service.ParsedConfig, endpoints []string, protocol string, securityCfg connection.SecurityConfig) (connection.Config, error) {
	maxSize, err := conf.FieldInt("connection_pool", "max_size")
	if err != nil {
		return connection.Config{}, fmt.Errorf("failed to parse connection_pool.max_size: %w", err)
	}
	idleTimeout, err := conf.FieldDuration("connection_pool", "idle_timeout")
	if err != nil {
		return connection.Config{}, fmt.Errorf("failed to parse connection_pool.idle_timeout: %w", err)
	}
	healthCheckInterval, err := conf.FieldDuration("connection_pool", "health_check_interval")
	if err != nil {
		return connection.Config{}, fmt.Errorf("failed to parse connection_pool.health_check_interval: %w", err)
	}
	connectTimeout, err := conf.FieldDuration("connection_pool", "connect_timeout")
	if err != nil {
		return connection.Config{}, fmt.Errorf("failed to parse connection_pool.connect_timeout: %w", err)
	}

	return connection.Config{
		Endpoints:           endpoints,
		Protocol:            protocol,
		MaxPoolSize:         maxSize,
		IdleTimeout:         idleTimeout,
		HealthCheckInterval: healthCheckInterval,
		ConnectTimeout:      connectTimeout,
		Security:            securityCfg,
	}, nil
}

func parseObserverConfig(conf *service.ParsedConfig, observePaths []string) (observer.Config, error) {
	bufferSize, err := conf.FieldInt("observer", "buffer_size")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse observer.buffer_size: %w", err)
	}
	observeTimeout, err := conf.FieldDuration("observer", "observe_timeout")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse observer.observe_timeout: %w", err)
	}
	resubscribeDelay, err := conf.FieldDuration("observer", "resubscribe_delay")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse observer.resubscribe_delay: %w", err)
	}

	// Parse retry policy
	maxRetries, err := conf.FieldInt("retry_policy", "max_retries")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse retry_policy.max_retries: %w", err)
	}
	initialInterval, err := conf.FieldDuration("retry_policy", "initial_interval")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse retry_policy.initial_interval: %w", err)
	}
	maxInterval, err := conf.FieldDuration("retry_policy", "max_interval")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse retry_policy.max_interval: %w", err)
	}
	multiplier, err := conf.FieldFloat("retry_policy", "multiplier")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse retry_policy.multiplier: %w", err)
	}
	jitter, err := conf.FieldBool("retry_policy", "jitter")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse retry_policy.jitter: %w", err)
	}
	retryPolicy := observer.RetryPolicy{
		MaxRetries:      maxRetries,
		InitialInterval: initialInterval,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		Jitter:          jitter,
	}

	// Parse circuit breaker config
	cbEnabled, err := conf.FieldBool("circuit_breaker", "enabled")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse circuit_breaker.enabled: %w", err)
	}
	cbFailureThreshold, err := conf.FieldInt("circuit_breaker", "failure_threshold")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse circuit_breaker.failure_threshold: %w", err)
	}
	cbSuccessThreshold, err := conf.FieldInt("circuit_breaker", "success_threshold")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse circuit_breaker.success_threshold: %w", err)
	}
	cbTimeout, err := conf.FieldDuration("circuit_breaker", "timeout")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse circuit_breaker.timeout: %w", err)
	}
	cbHalfOpenMaxCalls, err := conf.FieldInt("circuit_breaker", "half_open_max_calls")
	if err != nil {
		return observer.Config{}, fmt.Errorf("failed to parse circuit_breaker.half_open_max_calls: %w", err)
	}
	circuitConfig := observer.CircuitConfig{
		Enabled:          cbEnabled,
		FailureThreshold: cbFailureThreshold,
		SuccessThreshold: cbSuccessThreshold,
		Timeout:          cbTimeout,
		HalfOpenMaxCalls: cbHalfOpenMaxCalls,
	}

	return observer.Config{
		ObservePaths:     observePaths,
		RetryPolicy:      retryPolicy,
		CircuitBreaker:   circuitConfig,
		BufferSize:       bufferSize,
		ObserveTimeout:   observeTimeout,
		ResubscribeDelay: resubscribeDelay,
	}, nil
}

func parseConverterConfig(conf *service.ParsedConfig) (converter.Config, error) {
	defaultContentFormat, err := conf.FieldString("converter", "default_content_format")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse converter.default_content_format: %w", err)
	}
	compressionEnabled, err := conf.FieldBool("converter", "compression_enabled")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse converter.compression_enabled: %w", err)
	}
	maxPayloadSize, err := conf.FieldInt("converter", "max_payload_size")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse converter.max_payload_size: %w", err)
	}
	preserveOptions, err := conf.FieldBool("converter", "preserve_options")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse converter.preserve_options: %w", err)
	}
	return converter.Config{
		DefaultContentFormat: defaultContentFormat,
		CompressionEnabled:   compressionEnabled,
		MaxPayloadSize:       maxPayloadSize,
		PreserveOptions:      preserveOptions,
	}, nil
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
	i.logger.Info(fmt.Sprintf("Attempting to start observer manager for endpoints: %v, paths: %v", i.connManager.Config().Endpoints, i.obsManager.Config().ObservePaths))
	if err := i.obsManager.Start(); err != nil {
		i.logger.Error(fmt.Sprintf("Failed to start observer manager for endpoints %v, paths %v: %v", i.connManager.Config().Endpoints, i.obsManager.Config().ObservePaths, err))
		return fmt.Errorf("failed to start observer manager: %w", err)
	}

	// Start message forwarding goroutine
	i.wg.Add(1)
	go i.forwardMessages()

	i.logger.Info("CoAP input connected successfully")
	return nil
}

func (i *Input) forwardMessages() {
	defer i.wg.Done() // Signal that this goroutine has finished when it exits
	defer func() {
		if r := recover(); r != nil {
			i.logger.Error(fmt.Sprintf("Message forwarding panic in CoAP input for endpoints %v: %v", i.connManager.Config().Endpoints, r))
			// Optionally, re-panic if you want the process to crash, or handle more gracefully
		}
	}()

	for {
		select {
		case msg, ok := <-i.obsManager.MessageChan():
			if !ok {
				i.logger.Debug(fmt.Sprintf("Observer message channel closed for CoAP input with endpoints: %v. Exiting forwardMessages.", i.connManager.Config().Endpoints))
				return
			}

			// Attempt to add metadata about the source of the message if possible
			// This depends on how messages are structured by the observer.
			// For example, if msg.MetaGet("coap_path") exists:
			// sourcePath, _ := msg.MetaGet("coap_path")

			select {
			case i.msgChan <- msg:
				i.metrics.MessagesRead.Incr(1)
			default:
				// Add more context to the log message
				observePaths := i.obsManager.Config().ObservePaths
				i.logger.Warn(fmt.Sprintf("Message buffer full for CoAP input, dropping message. Endpoints: %v, Observe Paths: %v", i.connManager.Config().Endpoints, observePaths))
				i.metrics.MessagesDropped.Incr(1)
			}

		case <-i.closeChan:
			i.logger.Debug(fmt.Sprintf("closeChan received in forwardMessages for CoAP input with endpoints: %v. Exiting.", i.connManager.Config().Endpoints))
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
				// Attempt to get message metadata for logging
				var msgDetails string
				if mStr, mErr := msg.AsBytes(); mErr == nil {
					msgDetails = fmt.Sprintf("Message ID: %s, Content snippet: %s", msg.ID(), string(mStr[:min(20, len(mStr))]))
				} else {
					msgDetails = fmt.Sprintf("Message ID: %s (content not available)", msg.ID())
				}
				i.logger.Error(fmt.Sprintf("Message processing failed for CoAP input. Endpoints: %v. Details: %s. Error: %v", i.connManager.Config().Endpoints, msgDetails, err))
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
		i.logger.Debug(fmt.Sprintf("Closing observer manager for CoAP input with endpoints: %v, paths: %v", i.connManager.Config().Endpoints, i.obsManager.Config().ObservePaths))
		if err := i.obsManager.Close(); err != nil {
			i.logger.Error(fmt.Sprintf("Failed to close observer manager for CoAP input with endpoints %v, paths %v: %v", i.connManager.Config().Endpoints, i.obsManager.Config().ObservePaths, err))
		}

		// Close connection manager
		i.logger.Debug(fmt.Sprintf("Closing connection manager for CoAP input with endpoints: %v", i.connManager.Config().Endpoints))
		if i.connManager != nil {
			if err := i.connManager.Close(); err != nil {
				i.logger.Error(fmt.Sprintf("Failed to close connection manager for CoAP input with endpoints %v: %v", i.connManager.Config().Endpoints, err))
			}
		}

		// Wait for forwardMessages goroutine to finish.
		// This ensures that no more messages will be written to i.msgChan.
		i.logger.Debug("Waiting for forwardMessages goroutine to stop...")
		i.wg.Wait()
		i.logger.Debug("forwardMessages goroutine stopped.")

		// Now it's safe to close i.msgChan as there are no more writers.
		// The Read method will stop due to i.closeChan.
		// Any messages already in i.msgChan will be processed by Read calls
		// that were initiated before i.closeChan was processed by them,
		// or they will be drained if Benthos stops calling Read.
		// Explicitly closing msgChan is good practice if there were other consumers or complex scenarios.
		// However, given Benthos's Read loop and our handling of i.closeChan in Read,
		// the explicit draining goroutine might have been redundant if msgChan was closed.
		// Let's ensure it's closed to prevent any theoretical leaks with the old draining goroutine.
		// But we'll remove the old draining goroutine.
		close(i.msgChan)
		i.logger.Debug("Closed input msgChan.")

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
		"status":        "healthy",
		"buffer_usage":  fmt.Sprintf("%d/%d", len(i.msgChan), cap(i.msgChan)),
		"endpoints":     i.connManager.Config().Endpoints,
		"observe_paths": i.obsManager.Config().ObservePaths,
	}

	if i.obsManager != nil {
		status["observers"] = i.obsManager.HealthStatus()
	}

	return status
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
