// pkg/output/coap.go
package output

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"
	udpClient "github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
	"github.com/twinfer/benthos-coap-plugin/pkg/utils"
)

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Sends messages to CoAP endpoints.").
		Description("The CoAP output sends Benthos messages as CoAP requests to specified endpoints. Supports UDP, TCP, DTLS, and TCP-TLS protocols with connection pooling and retry mechanisms.").
		Field(service.NewStringListField("endpoints").
			Description("List of CoAP endpoints to send messages to.").
			Example([]string{"coap://localhost:5683", "coaps://device.local:5684"})).
		Field(service.NewStringField("default_path").
			Description("Default resource path for messages without explicit path.").
			Default("/").
			Example("/data/events")).
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
		Field(service.NewObjectField("request_options",
			service.NewBoolField("confirmable").
				Description("Send confirmable messages requiring acknowledgment.").
				Default(true),
			service.NewStringField("default_method").
				Description("Default CoAP method if not specified in message metadata.").
				Default("POST").
				LintRule("root in ['GET', 'POST', 'PUT', 'DELETE']"),
			service.NewDurationField("timeout").
				Description("Request timeout for confirmable messages.").
				Default("30s"),
			service.NewStringField("content_format").
				Description("Default content format for messages.").
				Default("application/json").
				Optional(),
			service.NewBoolField("auto_detect_format").
				Description("Automatically detect content format from payload.").
				Default(true),
		).Description("CoAP request options.")).
		Field(service.NewObjectField("retry_policy",
			service.NewIntField("max_retries").
				Description("Maximum number of retry attempts.").
				Default(3),
			service.NewDurationField("initial_interval").
				Description("Initial retry interval.").
				Default("500ms"),
			service.NewDurationField("max_interval").
				Description("Maximum retry interval.").
				Default("10s"),
			service.NewFloatField("multiplier").
				Description("Retry interval multiplier.").
				Default(1.5),
			service.NewBoolField("jitter").
				Description("Add random jitter to retry intervals.").
				Default(true),
		).Description("Retry policy for failed requests.")).
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

	err := service.RegisterOutput("coap", configSpec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		output, err := newCoAPOutput(conf, mgr)
		if err != nil {
			return nil, 0, err
		}
		return output, 1, nil
	})
	if err != nil {
		panic(err)
	}
}

type Output struct {
	connManager *connection.Manager
	converter   *converter.Converter
	config      OutputConfig
	logger      *service.Logger
	metrics     *Metrics
	mu          sync.RWMutex
}

type OutputConfig struct {
	Endpoints      []string
	DefaultPath    string
	Protocol       string
	Security       connection.SecurityConfig
	ConnectionPool connection.Config
	RequestOptions RequestOptions
	RetryPolicy    RetryPolicy
	Converter      converter.Config
}

type RequestOptions struct {
	Confirmable      bool
	DefaultMethod    string
	Timeout          time.Duration
	ContentFormat    string
	AutoDetectFormat bool
}

type RetryPolicy struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Jitter          bool
}

type Metrics struct {
	MessagesSent    *service.MetricCounter
	MessagesFailed  *service.MetricCounter
	RequestsTotal   *service.MetricCounter
	RequestsSuccess *service.MetricCounter
	RequestsTimeout *service.MetricCounter
	RetriesTotal    *service.MetricCounter
	ConnectionsUsed *service.MetricCounter
}

func newCoAPOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, error) {
	// Parse configuration
	endpoints, err := conf.FieldStringList("endpoints")
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoints: %w", err)
	}

	defaultPath, err := conf.FieldString("default_path")
	if err != nil {
		return nil, fmt.Errorf("failed to parse default_path: %w", err)
	}

	protocol, err := conf.FieldString("protocol")
	if err != nil {
		return nil, fmt.Errorf("failed to parse protocol: %w", err)
	}

	// Parse security config
	security, err := parseOutputSecurityConfig(conf)
	if err != nil {
		return nil, err
	}

	// Parse connection config (includes pool settings)
	connConfig, err := parseOutputConnectionConfig(conf, endpoints, protocol, security)
	if err != nil {
		return nil, err
	}

	// Parse request options
	requestOptions, err := parseRequestOptions(conf)
	if err != nil {
		return nil, err
	}

	// Parse retry policy
	retryPolicy, err := parseOutputRetryPolicy(conf)
	if err != nil {
		return nil, err
	}

	// Parse converter config
	converterConfig, err := parseOutputConverterConfig(conf)
	if err != nil {
		return nil, err
	}

	// Initialize components
	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager for endpoints %v: %w", endpoints, err)
	}

	conv := converter.NewConverter(converterConfig, mgr.Logger())

	outputConfig := OutputConfig{
		Endpoints:      endpoints,
		DefaultPath:    defaultPath,
		Protocol:       protocol,
		Security:       security,
		ConnectionPool: connConfig,
		RequestOptions: requestOptions,
		RetryPolicy:    retryPolicy,
		Converter:      converterConfig,
	}

	output := &Output{
		connManager: connManager,
		converter:   conv,
		config:      outputConfig,
		logger:      mgr.Logger(),
		metrics: &Metrics{
			MessagesSent:    mgr.Metrics().NewCounter("coap_output_messages_sent"),
			MessagesFailed:  mgr.Metrics().NewCounter("coap_output_messages_failed"),
			RequestsTotal:   mgr.Metrics().NewCounter("coap_output_requests_total"),
			RequestsSuccess: mgr.Metrics().NewCounter("coap_output_requests_success"),
			RequestsTimeout: mgr.Metrics().NewCounter("coap_output_requests_timeout"),
			RetriesTotal:    mgr.Metrics().NewCounter("coap_output_retries_total"),
			ConnectionsUsed: mgr.Metrics().NewCounter("coap_output_connections_used"),
		},
	}

	return output, nil
}

func (o *Output) Connect(ctx context.Context) error {
	o.logger.Info(fmt.Sprintf("Connecting CoAP output to endpoints: %v", o.config.Endpoints))
	return nil
}

func (o *Output) Write(ctx context.Context, msg *service.Message) error {
	return o.WriteWithRetry(ctx, msg, 0)
}

func (o *Output) WriteWithRetry(ctx context.Context, msg *service.Message, retryCount int) error {
	o.metrics.RequestsTotal.Incr(1)

	// Get connection from pool
	conn, err := o.connManager.Get(ctx)
	if err != nil {
		// Failed to get connection
		return fmt.Errorf("failed to get CoAP connection (endpoints: %v): %w", o.config.Endpoints, err)
	}
	defer o.connManager.Put(conn)

	o.metrics.ConnectionsUsed.Incr(1)
	endpoint := conn.Endpoint() // For logging

	// Determine path
	path, pathExists := msg.MetaGet("coap_path")
	if !pathExists {
		if o.config.DefaultPath == "" {
			o.logger.Warnf("No coap_path in message metadata and no default_path configured for endpoint %s. Sending to '/'.", endpoint)
			path = "/" // Fallback if truly no path can be determined.
		} else {
			path = o.config.DefaultPath
		}
		msg.MetaSet("coap_path", path) // Set for clarity and potential re-conversion
	}

	// Convert Benthos message to CoAP message
	coapMsg, err := o.converter.MessageToCoAP(msg) // converter now uses coap_path from metadata
	if err != nil {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("failed to convert Benthos message to CoAP message for path %s on endpoint %s: %w", path, endpoint, err)
	}

	// Configure request type
	if o.config.RequestOptions.Confirmable {
		coapMsg.Type = message.Confirmable
	} else {
		coapMsg.Type = message.NonConfirmable
	}

	// Send CoAP request
	// Pass original Benthos message to sendCoAPMessage for metadata access
	err = o.sendCoAPMessage(ctx, conn, msg, coapMsg, path)
	if err != nil {
		if retryCount < o.config.RetryPolicy.MaxRetries {
			o.metrics.RetriesTotal.Incr(1)
			backoffConfig := utils.BackoffConfig{
				InitialInterval: o.config.RetryPolicy.InitialInterval,
				MaxInterval:     o.config.RetryPolicy.MaxInterval,
				Multiplier:      o.config.RetryPolicy.Multiplier,
				Jitter:          o.config.RetryPolicy.Jitter,
			}
			delay := utils.CalculateBackoff(retryCount, backoffConfig)
			tokenStr := ""
			if len(coapMsg.Token) > 0 {
				tokenStr = string(coapMsg.Token)
			}

			o.logger.Warnf("CoAP request failed to path %s on endpoint %s (token: %s), retrying (%d/%d) in %v: %v",
				path, endpoint, tokenStr, retryCount+1, o.config.RetryPolicy.MaxRetries, delay, err)

			select {
			case <-time.After(delay):
				return o.WriteWithRetry(ctx, msg, retryCount+1)
			case <-ctx.Done():
				o.logger.Warnf("Context done during retry backoff to path %s on endpoint %s: %v", path, endpoint, ctx.Err())
				return ctx.Err()
			}
		}

		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("CoAP request failed to path %s on endpoint %s after %d retries: %w", path, endpoint, retryCount, err)
	}

	o.metrics.MessagesSent.Incr(1)
	o.metrics.RequestsSuccess.Incr(1)
	return nil
}

func (o *Output) sendCoAPMessage(ctx context.Context, connWrapper *connection.ConnectionWrapper, originalBenthosMsg *service.Message, coapMsg *message.Message, path string) error {

	endpoint := connWrapper.Endpoint()
	tokenStr := ""
	if len(coapMsg.Token) > 0 {
		tokenStr = string(coapMsg.Token)
	}

	o.logger.Debugf("Sending CoAP message to path %s on endpoint %s (token: %s, type: %s)", path, endpoint, tokenStr, coapMsg.Type.String())

	rawConn := connWrapper.Connection()
	protocol := connWrapper.Protocol()
	endpoint = connWrapper.Endpoint()
	tokenStr = coapMsg.Token.String() // Use String() for proper token representation

	// Create a new context with the request timeout
	reqCtx, cancel := context.WithTimeout(ctx, o.config.RequestOptions.Timeout)
	defer cancel()

	o.logger.Debugf("Attempting to send CoAP message to path %s on endpoint %s (token: %s, type: %s, protocol: %s)",
		path, endpoint, tokenStr, coapMsg.Type.String(), protocol)

	switch c := rawConn.(type) {
	case *udpClient.Conn: // Handles UDP and DTLS
		return o.sendUDPMessage(reqCtx, c, originalBenthosMsg, coapMsg, endpoint, path, tokenStr)
	case *tcpClient.Conn: // Handles TCP and TCP-TLS
		return o.sendTCPMessage(reqCtx, c, originalBenthosMsg, coapMsg, endpoint, path, tokenStr)
	default:
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("unsupported connection type for CoAP output: %T (protocol: %s)", rawConn, protocol)
	}
}

// sendUDPMessage sends a CoAP message using a UDP client connection.
func (o *Output) sendUDPMessage(ctx context.Context, conn *udpClient.Conn, originalBenthosMsg *service.Message, coapMsg *message.Message, endpoint, path, tokenStr string) error {
	// Create a standalone message (no pool release needed)
	req := pool.NewMessage(ctx)

	// Determine CoAP method
	methodStr, _ := originalBenthosMsg.MetaGet("coap_method")
	if methodStr == "" {
		methodStr = o.config.RequestOptions.DefaultMethod
	}
	coapCode, err := methodStringToCode(methodStr)
	if err != nil {
		o.logger.Warnf("Invalid CoAP method '%s' for endpoint %s: %v. Defaulting to POST.", methodStr, endpoint, err)
		coapCode = codes.POST
	}

	// Set basic message properties
	req.SetCode(coapCode)
	req.SetType(coapMsg.Type)
	req.SetToken(coapMsg.Token)

	// Set the path
	if err := req.SetPath(path); err != nil {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("failed to set path %s for UDP request to %s: %w", path, endpoint, err)
	}

	// Set payload using SetBody instead of SetPayload
	req.SetBody(bytes.NewReader(coapMsg.Payload))

	// Copy content format if set
	if len(coapMsg.Options) > 0 {
		// Try to extract content format from options
		for _, opt := range coapMsg.Options {
			if opt.ID == message.ContentFormat && len(opt.Value) > 0 {
				// Convert bytes to uint32 for content format
				if len(opt.Value) >= 1 {
					cf := message.MediaType(opt.Value[0])
					req.SetContentFormat(cf)
				}
			}
		}
	}

	o.logger.Debugf("Sending UDP CoAP POST request to path %s on endpoint %s (token: %s)", path, endpoint, tokenStr)

	// Send the request
	resp, err := conn.Do(req)
	if err != nil {
		o.metrics.MessagesFailed.Incr(1)
		// Check if the error is due to context timeout
		if ctx.Err() == context.DeadlineExceeded {
			o.metrics.RequestsTimeout.Incr(1)
			return fmt.Errorf("CoAP UDP request to %s on %s timed out (token: %s): %w", path, endpoint, tokenStr, err)
		}
		return fmt.Errorf("CoAP UDP request to %s on %s failed (token: %s): %w", path, endpoint, tokenStr, err)
	}
	// No need to release standalone response message

	respCode := resp.Code()
	o.logger.Debugf("Received UDP CoAP response from %s on %s (token: %s, code: %s)",
		path, endpoint, tokenStr, respCode.String())

	// Check response code (2.xx is success)
	if respCode >= codes.BadRequest {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("CoAP UDP request to %s on %s returned error code %s (token: %s)", path, endpoint, respCode.String(), tokenStr)
	}

	return nil
}

// sendTCPMessage sends a CoAP message using a TCP client connection.
func (o *Output) sendTCPMessage(ctx context.Context, conn *tcpClient.Conn, originalBenthosMsg *service.Message, coapMsg *message.Message, endpoint, path, tokenStr string) error {
	// Create a standalone message (no pool release needed)
	req := pool.NewMessage(ctx)

	// Determine CoAP method
	methodStr, _ := originalBenthosMsg.MetaGet("coap_method")
	if methodStr == "" {
		methodStr = o.config.RequestOptions.DefaultMethod
	}
	coapCode, err := methodStringToCode(methodStr)
	if err != nil {
		o.logger.Warnf("Invalid CoAP method '%s' for endpoint %s: %v. Defaulting to POST.", methodStr, endpoint, err)
		coapCode = codes.POST
	}

	// Set basic message properties
	req.SetCode(coapCode)
	req.SetType(coapMsg.Type)
	req.SetToken(coapMsg.Token)

	// Set the path
	if err := req.SetPath(path); err != nil {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("failed to set path %s for TCP request to %s: %w", path, endpoint, err)
	}

	// Set payload using SetBody instead of SetPayload
	req.SetBody(bytes.NewReader(coapMsg.Payload))

	// Copy content format if set
	if len(coapMsg.Options) > 0 {
		// Try to extract content format from options
		for _, opt := range coapMsg.Options {
			if opt.ID == message.ContentFormat && len(opt.Value) > 0 {
				// Convert bytes to uint32 for content format
				if len(opt.Value) >= 1 {
					cf := message.MediaType(opt.Value[0])
					req.SetContentFormat(cf)
				}
			}
		}
	}

	o.logger.Debugf("Sending TCP CoAP POST request to path %s on endpoint %s (token: %s)", path, endpoint, tokenStr)

	// Send the request
	resp, err := conn.Do(req)
	if err != nil {
		o.metrics.MessagesFailed.Incr(1)
		if ctx.Err() == context.DeadlineExceeded {
			o.metrics.RequestsTimeout.Incr(1)
			return fmt.Errorf("CoAP TCP request to %s on %s timed out (token: %s): %w", path, endpoint, tokenStr, err)
		}
		return fmt.Errorf("CoAP TCP request to %s on %s failed (token: %s): %w", path, endpoint, tokenStr, err)
	}
	// No need to release standalone response message

	respCode := resp.Code()
	o.logger.Debugf("Received TCP CoAP response from %s on %s (token: %s, code: %s)",
		path, endpoint, tokenStr, respCode.String())

	// Check response code (2.xx is success)
	if respCode >= codes.BadRequest {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("CoAP TCP request to %s on %s returned error code %s (token: %s)", path, endpoint, respCode.String(), tokenStr)
	}

	return nil
}

func (o *Output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	var firstErr error
	for i, msg := range batch {
		if err := o.Write(ctx, msg); err != nil {
			o.logger.Errorf("Failed to write message (index %d in batch) to CoAP endpoint: %v", i, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to write message (index %d in batch): %w", i, err)
			}
			// Decide on batch error handling: stop on first error or try all?
			// Current behavior: try all, return first error.
		}
	}
	return firstErr
}

func (o *Output) Close(ctx context.Context) error {
	o.logger.Infof("Closing CoAP output for endpoints: %v", o.config.Endpoints)

	if o.connManager != nil {
		if err := o.connManager.Close(); err != nil {
			o.logger.Errorf("Failed to close connection manager for CoAP output (endpoints %v): %v", o.config.Endpoints, err)
			// Decide if this error should prevent further cleanup or be returned.
			// For now, we log and continue, then return the error.
			return err
		}
	}

	o.logger.Infof("CoAP output closed for endpoints: %v", o.config.Endpoints)
	return nil
}

// Health returns the current health status of the output
func (o *Output) Health() map[string]interface{} {
	healthStatus := map[string]interface{}{
		"status":       "healthy", // Default to healthy, can be changed based on checks
		"endpoints":    o.config.Endpoints,
		"protocol":     o.config.Protocol,
		"default_path": o.config.DefaultPath,
	}

	if o.connManager != nil {
		// You might want to expand connManager.HealthStatus() to provide more details
		// For now, let's assume it returns a map that can be merged.
		// healthStatus["connection_pool"] = o.connManager.HealthStatus()
		// Simplified:
		healthStatus["connection_pool_status"] = "active" // Placeholder
	} else {
		healthStatus["status"] = "degraded"
		healthStatus["connection_pool_status"] = "inactive"
	}

	// Add more checks here if needed, e.g., try a ping to an endpoint
	// For now, "healthy" means the output component is configured and running.

	return healthStatus
}

func parseOutputSecurityConfig(conf *service.ParsedConfig) (connection.SecurityConfig, error) {
	securityMode, err := conf.FieldString("security", "mode")
	if err != nil {
		return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.mode: %w", err)
	}
	security := connection.SecurityConfig{
		Mode: securityMode,
	}

	if securityMode == "psk" {
		if pskIdentity, err := conf.FieldString("security", "psk_identity"); err == nil {
			security.PSKIdentity = pskIdentity
		}
		if pskKey, err := conf.FieldString("security", "psk_key"); err == nil {
			security.PSKKey = pskKey
		}
	} else if securityMode == "certificate" {
		if certFile, err := conf.FieldString("security", "cert_file"); err == nil {
			security.CertFile = certFile
		}
		if keyFile, err := conf.FieldString("security", "key_file"); err == nil {
			security.KeyFile = keyFile
		}
		if caCertFile, err := conf.FieldString("security", "ca_cert_file"); err == nil {
			security.CACertFile = caCertFile
		}
		security.InsecureSkip, err = conf.FieldBool("security", "insecure_skip_verify")
		if err != nil {
			return connection.SecurityConfig{}, fmt.Errorf("failed to parse security.insecure_skip_verify: %w", err)
		}
	}
	return security, nil
}

func parseOutputConnectionConfig(conf *service.ParsedConfig, endpoints []string, protocol string, securityCfg connection.SecurityConfig) (connection.Config, error) {
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

func parseOutputConverterConfig(conf *service.ParsedConfig) (converter.Config, error) {
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

func parseRequestOptions(conf *service.ParsedConfig) (RequestOptions, error) {
	confirmable, err := conf.FieldBool("request_options", "confirmable")
	if err != nil {
		return RequestOptions{}, fmt.Errorf("failed to parse request_options.confirmable: %w", err)
	}
	defaultMethod, err := conf.FieldString("request_options", "default_method")
	if err != nil {
		return RequestOptions{}, fmt.Errorf("failed to parse request_options.default_method: %w", err)
	}
	timeout, err := conf.FieldDuration("request_options", "timeout")
	if err != nil {
		return RequestOptions{}, fmt.Errorf("failed to parse request_options.timeout: %w", err)
	}
	autoDetectFormat, err := conf.FieldBool("request_options", "auto_detect_format")
	if err != nil {
		return RequestOptions{}, fmt.Errorf("failed to parse request_options.auto_detect_format: %w", err)
	}

	opts := RequestOptions{
		Confirmable:      confirmable,
		DefaultMethod:    defaultMethod,
		Timeout:          timeout,
		AutoDetectFormat: autoDetectFormat,
	}

	if contentFormat, err := conf.FieldString("request_options", "content_format"); err == nil {
		opts.ContentFormat = contentFormat
	}
	return opts, nil
}

// Helper function to convert method string to CoAP code
func methodStringToCode(method string) (codes.Code, error) {
	switch method {
	case "GET":
		return codes.GET, nil
	case "POST":
		return codes.POST, nil
	case "PUT":
		return codes.PUT, nil
	case "DELETE":
		return codes.DELETE, nil
	default:
		return codes.Empty, fmt.Errorf("unsupported CoAP method: %s", method)
	}
}

func parseOutputRetryPolicy(conf *service.ParsedConfig) (RetryPolicy, error) {
	maxRetries, err := conf.FieldInt("retry_policy", "max_retries")
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("failed to parse retry_policy.max_retries: %w", err)
	}
	initialInterval, err := conf.FieldDuration("retry_policy", "initial_interval")
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("failed to parse retry_policy.initial_interval: %w", err)
	}
	maxInterval, err := conf.FieldDuration("retry_policy", "max_interval")
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("failed to parse retry_policy.max_interval: %w", err)
	}
	multiplier, err := conf.FieldFloat("retry_policy", "multiplier")
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("failed to parse retry_policy.multiplier: %w", err)
	}
	jitter, err := conf.FieldBool("retry_policy", "jitter")
	if err != nil {
		return RetryPolicy{}, fmt.Errorf("failed to parse retry_policy.jitter: %w", err)
	}
	return RetryPolicy{
		MaxRetries:      maxRetries,
		InitialInterval: initialInterval,
		MaxInterval:     maxInterval,
		Multiplier:      multiplier,
		Jitter:          jitter,
	}, nil
}
