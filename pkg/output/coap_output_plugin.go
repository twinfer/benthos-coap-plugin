// pkg/output/coap.go
package output

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/tcp"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
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

	err := service.RegisterOutput("coap", configSpec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, error) {
		return newCoAPOutput(conf, mgr)
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

func newCoAPOutput(conf *service.ParsedConfig, mgr *service.Resources) (*Output, error) {
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

	connConfig := connection.Config{
		Endpoints:           endpoints,
		Protocol:            protocol,
		MaxPoolSize:         poolConf["max_size"].(int),
		IdleTimeout:         poolConf["idle_timeout"].(time.Duration),
		HealthCheckInterval: poolConf["health_check_interval"].(time.Duration),
		ConnectTimeout:      poolConf["connect_timeout"].(time.Duration),
		Security:            security,
	}

	// Parse request options
	reqOptsConf, err := conf.FieldObjectMap("request_options")
	if err != nil {
		return nil, fmt.Errorf("failed to parse request_options config: %w", err)
	}

	requestOptions := RequestOptions{
		Confirmable:      reqOptsConf["confirmable"].(bool),
		Timeout:          reqOptsConf["timeout"].(time.Duration),
		AutoDetectFormat: reqOptsConf["auto_detect_format"].(bool),
	}

	if contentFormat, exists := reqOptsConf["content_format"]; exists {
		requestOptions.ContentFormat = contentFormat.(string)
	}

	// Parse retry policy
	retryConf, err := conf.FieldObjectMap("retry_policy")
	if err != nil {
		return nil, fmt.Errorf("failed to parse retry_policy config: %w", err)
	}

	retryPolicy := RetryPolicy{
		MaxRetries:      retryConf["max_retries"].(int),
		InitialInterval: retryConf["initial_interval"].(time.Duration),
		MaxInterval:     retryConf["max_interval"].(time.Duration),
		Multiplier:      retryConf["multiplier"].(float64),
		Jitter:          retryConf["jitter"].(bool),
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
	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
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
			MessagesSent:    mgr.MetricCounter("coap_output_messages_sent"),
			MessagesFailed:  mgr.MetricCounter("coap_output_messages_failed"),
			RequestsTotal:   mgr.MetricCounter("coap_output_requests_total"),
			RequestsSuccess: mgr.MetricCounter("coap_output_requests_success"),
			RequestsTimeout: mgr.MetricCounter("coap_output_requests_timeout"),
			RetriesTotal:    mgr.MetricCounter("coap_output_retries_total"),
			ConnectionsUsed: mgr.MetricCounter("coap_output_connections_used"),
		},
	}

	return output, nil
}

func (o *Output) Connect(ctx context.Context) error {
	o.logger.Info("Connecting CoAP output", "endpoints", o.config.Endpoints)
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
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer o.connManager.Put(conn)

	o.metrics.ConnectionsUsed.Incr(1)

	// Convert Benthos message to CoAP message
	coapMsg, err := o.converter.MessageToCoAP(msg)
	if err != nil {
		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("failed to convert message: %w", err)
	}

	// Set default path if not specified in message metadata
	if path, exists := msg.MetaGet("coap_path"); exists {
		// Path already set in metadata
		_ = path
	} else if o.config.DefaultPath != "" {
		// Use default path
		msg.MetaSet("coap_path", o.config.DefaultPath)
		// Re-convert with path
		coapMsg, err = o.converter.MessageToCoAP(msg)
		if err != nil {
			return fmt.Errorf("failed to re-convert message with path: %w", err)
		}
	}

	// Configure request type
	if o.config.RequestOptions.Confirmable {
		coapMsg.SetType(message.Confirmable)
	} else {
		coapMsg.SetType(message.NonConfirmable)
	}

	// Send CoAP request
	err = o.sendCoAPMessage(ctx, conn, coapMsg)
	if err != nil {
		if retryCount < o.config.RetryPolicy.MaxRetries {
			o.metrics.RetriesTotal.Incr(1)
			delay := o.calculateBackoff(retryCount)

			o.logger.Warn("CoAP request failed, retrying",
				"error", err,
				"retry_count", retryCount+1,
				"delay", delay)

			select {
			case <-time.After(delay):
				return o.WriteWithRetry(ctx, msg, retryCount+1)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		o.metrics.MessagesFailed.Incr(1)
		return fmt.Errorf("CoAP request failed after %d retries: %w", retryCount, err)
	}

	o.metrics.MessagesSent.Incr(1)
	o.metrics.RequestsSuccess.Incr(1)
	return nil
}

func (o *Output) sendCoAPMessage(ctx context.Context, connWrapper *connection.ConnectionWrapper, coapMsg *message.Message) error {
	requestCtx, cancel := context.WithTimeout(ctx, o.config.RequestOptions.Timeout)
	defer cancel()

	switch conn := connWrapper.conn.(type) {
	case *udp.Conn:
		return o.sendUDPMessage(requestCtx, conn, coapMsg)
	case *tcp.Conn:
		return o.sendTCPMessage(requestCtx, conn, coapMsg)
	default:
		return fmt.Errorf("unsupported connection type: %T", conn)
	}
}

func (o *Output) sendUDPMessage(ctx context.Context, conn *udp.Conn, coapMsg *message.Message) error {
	if coapMsg.Type() == message.Confirmable {
		// Wait for acknowledgment
		resp, err := conn.DoWithMessage(ctx, coapMsg)
		if err != nil {
			return fmt.Errorf("UDP confirmable request failed: %w", err)
		}

		// Check response code
		if resp.Code().IsClientError() || resp.Code().IsServerError() {
			return fmt.Errorf("CoAP error response: %s", resp.Code())
		}

		o.logger.Debug("UDP confirmable request successful", "response_code", resp.Code())
	} else {
		// Fire and forget
		err := conn.WriteMessage(coapMsg)
		if err != nil {
			return fmt.Errorf("UDP non-confirmable request failed: %w", err)
		}

		o.logger.Debug("UDP non-confirmable request sent")
	}

	return nil
}

func (o *Output) sendTCPMessage(ctx context.Context, conn *tcp.Conn, coapMsg *message.Message) error {
	// TCP is always reliable, so treat all messages as confirmable
	resp, err := conn.DoWithMessage(ctx, coapMsg)
	if err != nil {
		return fmt.Errorf("TCP request failed: %w", err)
	}

	// Check response code
	if resp.Code().IsClientError() || resp.Code().IsServerError() {
		return fmt.Errorf("CoAP error response: %s", resp.Code())
	}

	o.logger.Debug(fmt.Sprintf("TCP request successful, response_code: %v", resp.Code()))
	return nil
}

func (o *Output) calculateBackoff(retryCount int) time.Duration {
	delay := float64(o.config.RetryPolicy.InitialInterval)

	for i := 0; i < retryCount; i++ {
		delay *= o.config.RetryPolicy.Multiplier
	}

	if time.Duration(delay) > o.config.RetryPolicy.MaxInterval {
		delay = float64(o.config.RetryPolicy.MaxInterval)
	}

	if o.config.RetryPolicy.Jitter {
		// Add up to 25% jitter
		jitter := delay * 0.25 * (2*float64(time.Now().UnixNano()%100)/100 - 1)
		delay += jitter
	}

	return time.Duration(delay)
}

func (o *Output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	for i, msg := range batch {
		if err := o.Write(ctx, msg); err != nil {
			return fmt.Errorf("failed to write message %d in batch: %w", i, err)
		}
	}
	return nil
}

func (o *Output) Close(ctx context.Context) error {
	o.logger.Info("Closing CoAP output")

	if err := o.connManager.Close(); err != nil {
		o.logger.Error(fmt.Sprintf("Failed to close connection manager: %v", err))
		return err
	}

	o.logger.Info("CoAP output closed")
	return nil
}

// Health returns the current health status of the output
func (o *Output) Health() map[string]interface{} {
	return map[string]interface{}{
		"status":      "healthy",
		"endpoints":   o.config.Endpoints,
		"protocol":    o.config.Protocol,
		"connections": "available", // Could be enhanced with actual pool status
	}
}
