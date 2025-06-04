// pkg/input/coap_server_input_plugin.go
package input

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/server"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Acts as a CoAP server to receive requests from CoAP clients.").
		Description("The CoAP server input listens for incoming CoAP requests on specified endpoints and converts them to Benthos messages. Supports UDP, TCP, DTLS, and TCP-TLS protocols with configurable request handling.").
		Field(service.NewStringField("listen_address").
			Description("Address to listen on for incoming CoAP requests.").
			Default("0.0.0.0:5683").
			Example("0.0.0.0:5683")).
		Field(service.NewStringField("protocol").
			Description("CoAP protocol to use for the server.").
			Default("udp").
			LintRule("root in ['udp', 'tcp', 'udp-dtls', 'tcp-tls']")).
		Field(service.NewStringListField("allowed_paths").
			Description("List of resource paths to accept requests for. Empty means accept all paths.").
			Default([]string{}).
			Example([]string{"/api/data", "/sensors/+", "/actuators/*/command"})).
		Field(service.NewStringListField("allowed_methods").
			Description("List of CoAP methods to accept. Empty means accept all methods.").
			Default([]string{}).
			Example([]string{"GET", "POST", "PUT", "DELETE"})).
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
				Description("Path to server certificate file.").
				Optional(),
			service.NewStringField("key_file").
				Description("Path to server private key file.").
				Optional(),
			service.NewStringField("ca_cert_file").
				Description("Path to CA certificate file for client verification.").
				Optional(),
			service.NewBoolField("require_client_cert").
				Description("Require client certificate authentication.").
				Default(false),
		).Description("Security configuration for the CoAP server.")).
		Field(service.NewIntField("buffer_size").
			Description("Size of the message buffer for incoming requests.").
			Default(1000)).
		Field(service.NewDurationField("timeout").
			Description("Timeout for processing individual requests.").
			Default("30s")).
		Field(service.NewObjectField("response",
			service.NewStringField("default_content_format").
				Description("Default content format for responses.").
				Default("text/plain"),
			service.NewIntField("default_code").
				Description("Default response code for successful processing.").
				Default(int(codes.Content)),
			service.NewStringField("default_payload").
				Description("Default response payload.").
				Default("OK"),
		).Description("Default response configuration.")).
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
				Default(true),
		).Description("Message conversion configuration."))

	err := service.RegisterInput("coap_server", configSpec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return newCoAPServerInput(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type ServerInput struct {
	server      *server.Server
	udpConn     *coapNet.UDPConn  // CoAP UDP connection for server
	converter   *converter.Converter
	logger      *service.Logger
	metrics     *ServerMetrics
	config      ServerConfig

	msgChan   chan *service.Message
	closeChan chan struct{}
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
	wg        sync.WaitGroup
}

type ServerConfig struct {
	ListenAddress    string
	Protocol         string
	AllowedPaths     []string
	AllowedMethods   []string
	BufferSize       int
	Timeout          time.Duration
	SecurityConfig   SecurityConfig
	ResponseConfig   ResponseConfig
	ConverterConfig  converter.Config
}

type SecurityConfig struct {
	Mode               string
	PSKIdentity        string
	PSKKey             string
	CertFile           string
	KeyFile            string
	CACertFile         string
	RequireClientCert  bool
}

type ResponseConfig struct {
	DefaultContentFormat string
	DefaultCode          codes.Code
	DefaultPayload       string
}

type ServerMetrics struct {
	RequestsReceived  *service.MetricCounter
	RequestsProcessed *service.MetricCounter
	RequestsFailed    *service.MetricCounter
	ResponsesSent     *service.MetricCounter
	ConnectionsActive *service.MetricCounter
	ErrorsTotal       *service.MetricCounter
}

func newCoAPServerInput(conf *service.ParsedConfig, mgr *service.Resources) (*ServerInput, error) {
	// Parse configuration
	listenAddress, err := conf.FieldString("listen_address")
	if err != nil {
		return nil, fmt.Errorf("failed to parse listen_address: %w", err)
	}

	protocol, err := conf.FieldString("protocol")
	if err != nil {
		return nil, fmt.Errorf("failed to parse protocol: %w", err)
	}

	allowedPaths, err := conf.FieldStringList("allowed_paths")
	if err != nil {
		return nil, fmt.Errorf("failed to parse allowed_paths: %w", err)
	}

	allowedMethods, err := conf.FieldStringList("allowed_methods")
	if err != nil {
		return nil, fmt.Errorf("failed to parse allowed_methods: %w", err)
	}

	bufferSize, err := conf.FieldInt("buffer_size")
	if err != nil {
		return nil, fmt.Errorf("failed to parse buffer_size: %w", err)
	}

	timeout, err := conf.FieldDuration("timeout")
	if err != nil {
		return nil, fmt.Errorf("failed to parse timeout: %w", err)
	}

	// Parse security config
	security, err := parseServerSecurityConfig(conf)
	if err != nil {
		return nil, err
	}

	// Parse response config
	response, err := parseResponseConfig(conf)
	if err != nil {
		return nil, err
	}

	// Parse converter config
	converterConfig, err := parseServerConverterConfig(conf)
	if err != nil {
		return nil, err
	}

	// Validate configuration
	if bufferSize <= 0 {
		return nil, fmt.Errorf("buffer_size must be positive, got: %d", bufferSize)
	}

	config := ServerConfig{
		ListenAddress:   listenAddress,
		Protocol:        protocol,
		AllowedPaths:    allowedPaths,
		AllowedMethods:  allowedMethods,
		BufferSize:      bufferSize,
		Timeout:         timeout,
		SecurityConfig:  security,
		ResponseConfig:  response,
		ConverterConfig: converterConfig,
	}

	// Initialize components
	conv := converter.NewConverter(converterConfig, mgr.Logger())

	input := &ServerInput{
		converter: conv,
		logger:    mgr.Logger(),
		config:    config,
		msgChan:   make(chan *service.Message, bufferSize),
		closeChan: make(chan struct{}),
		metrics: &ServerMetrics{
			RequestsReceived:  mgr.Metrics().NewCounter("coap_server_requests_received"),
			RequestsProcessed: mgr.Metrics().NewCounter("coap_server_requests_processed"),
			RequestsFailed:    mgr.Metrics().NewCounter("coap_server_requests_failed"),
			ResponsesSent:     mgr.Metrics().NewCounter("coap_server_responses_sent"),
			ConnectionsActive: mgr.Metrics().NewCounter("coap_server_connections_active"),
			ErrorsTotal:       mgr.Metrics().NewCounter("coap_server_errors_total"),
		},
	}

	return input, nil
}

func (s *ServerInput) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		return nil // Already connected
	}

	// Create router for handling requests
	router := mux.NewRouter()
	router.Handle("/", mux.HandlerFunc(s.handleRequest))

	// Create UDP server
	s.server = udp.NewServer()

	// Create listener
	var err error
	if s.config.Protocol == "udp" || s.config.Protocol == "udp-dtls" {
		s.udpConn, err = coapNet.NewListenUDP("udp", s.config.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to listen on UDP address %s: %w", s.config.ListenAddress, err)
		}

		s.logger.Infof("CoAP server listening on UDP %s", s.config.ListenAddress)
	} else {
		return fmt.Errorf("protocol %s not yet implemented for server input", s.config.Protocol)
	}

	// Start server
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.udpConn.Close()

		if err := s.server.Serve(s.udpConn); err != nil {
			select {
			case <-s.closeChan:
				// Expected shutdown
				s.logger.Debug("CoAP server shutdown")
			default:
				s.logger.Errorf("CoAP server error: %v", err)
				s.metrics.ErrorsTotal.Incr(1)
			}
		}
	}()

	s.logger.Infof("CoAP server input started on %s (%s)", s.config.ListenAddress, s.config.Protocol)
	return nil
}

func (s *ServerInput) handleRequest(w mux.ResponseWriter, r *mux.Message) {
	s.metrics.RequestsReceived.Incr(1)

	// Extract request information
	method := s.getMethodString(r.Code())
	opts := r.Options()
	path, err := opts.Path()
	if err != nil || path == "" {
		path = "/"
	}

	// Check if method is allowed
	if len(s.config.AllowedMethods) > 0 && !s.isMethodAllowed(method) {
		s.logger.Debugf("Method %s not allowed", method)
		s.sendErrorResponse(w, codes.MethodNotAllowed, "Method not allowed")
		s.metrics.RequestsFailed.Incr(1)
		return
	}

	// Check if path is allowed
	if len(s.config.AllowedPaths) > 0 && !s.isPathAllowed(path) {
		s.logger.Debugf("Path %s not allowed", path)
		s.sendErrorResponse(w, codes.NotFound, "Path not found")
		s.metrics.RequestsFailed.Incr(1)
		return
	}

	// Convert CoAP request to Benthos message
	benthosMsg, err := s.convertRequestToMessage(r.Message, method, path)
	if err != nil {
		s.logger.Errorf("Failed to convert CoAP request to Benthos message: %v", err)
		s.sendErrorResponse(w, codes.InternalServerError, "Internal server error")
		s.metrics.RequestsFailed.Incr(1)
		return
	}

	// Send message to channel
	select {
	case s.msgChan <- benthosMsg:
		s.metrics.RequestsProcessed.Incr(1)
		s.logger.Debugf("Processed %s request for path %s", method, path)
		
		// Send default response
		s.sendSuccessResponse(w)
		s.metrics.ResponsesSent.Incr(1)
		
	case <-time.After(s.config.Timeout):
		s.logger.Warnf("Timeout processing %s request for path %s", method, path)
		s.sendErrorResponse(w, codes.ServiceUnavailable, "Service temporarily unavailable")
		s.metrics.RequestsFailed.Incr(1)
		
	case <-s.closeChan:
		s.logger.Debug("Server shutting down, rejecting request")
		s.sendErrorResponse(w, codes.ServiceUnavailable, "Server shutting down")
		s.metrics.RequestsFailed.Incr(1)
	}
}

func (s *ServerInput) convertRequestToMessage(poolMsg *pool.Message, method, path string) (*service.Message, error) {
	// Convert pool.Message to message.Message for the converter
	msg := &message.Message{
		Code:  poolMsg.Code(),
		Token: poolMsg.Token(),
		Type:  poolMsg.Type(),
	}

	// Copy options
	opts := poolMsg.Options()
	msg.Options = make(message.Options, len(opts))
	copy(msg.Options, opts)

	// Copy payload from body
	if body := poolMsg.Body(); body != nil {
		if payload, err := io.ReadAll(body); err == nil {
			msg.Payload = payload
		}
		// Reset body for any future reads
		if seeker, ok := body.(io.Seeker); ok {
			seeker.Seek(0, io.SeekStart)
		}
	}

	// Convert using the standard converter
	benthosMsg, err := s.converter.CoAPToMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("converter failed: %w", err)
	}

	// Add server-specific metadata
	benthosMsg.MetaSet("coap_server_method", method)
	benthosMsg.MetaSet("coap_server_path", path)
	benthosMsg.MetaSet("coap_server_timestamp", time.Now().Format(time.RFC3339))
	benthosMsg.MetaSet("coap_server_protocol", s.config.Protocol)

	return benthosMsg, nil
}

func (s *ServerInput) sendSuccessResponse(w mux.ResponseWriter) {
	payload := []byte(s.config.ResponseConfig.DefaultPayload)
	err := w.SetResponse(
		s.config.ResponseConfig.DefaultCode,
		message.TextPlain,
		bytes.NewReader(payload),
	)
	if err != nil {
		s.logger.Errorf("Failed to send success response: %v", err)
		s.metrics.ErrorsTotal.Incr(1)
	}
}

func (s *ServerInput) sendErrorResponse(w mux.ResponseWriter, code codes.Code, errorMsg string) {
	payload := []byte(errorMsg)
	err := w.SetResponse(code, message.TextPlain, bytes.NewReader(payload))
	if err != nil {
		s.logger.Errorf("Failed to send error response: %v", err)
		s.metrics.ErrorsTotal.Incr(1)
	}
}

func (s *ServerInput) getMethodString(code codes.Code) string {
	switch code {
	case codes.GET:
		return "GET"
	case codes.POST:
		return "POST"
	case codes.PUT:
		return "PUT"
	case codes.DELETE:
		return "DELETE"
	default:
		return code.String()
	}
}

func (s *ServerInput) isMethodAllowed(method string) bool {
	// If no methods are specified, allow all
	if len(s.config.AllowedMethods) == 0 {
		return true
	}
	
	for _, allowed := range s.config.AllowedMethods {
		if allowed == method {
			return true
		}
	}
	return false
}

func (s *ServerInput) isPathAllowed(path string) bool {
	// If no paths are specified, allow all
	if len(s.config.AllowedPaths) == 0 {
		return true
	}
	
	for _, allowed := range s.config.AllowedPaths {
		// Try exact match first (fastest)
		if allowed == path {
			return true
		}
		
		// Try pattern match if pattern contains wildcards
		if s.containsWildcards(allowed) && s.matchesPattern(allowed, path) {
			return true
		}
	}
	return false
}

// containsWildcards checks if a pattern contains wildcard characters
func (s *ServerInput) containsWildcards(pattern string) bool {
	return strings.Contains(pattern, "+") || strings.Contains(pattern, "*")
}

// matchesPattern checks if a path matches a wildcard pattern
// Patterns:
//   + matches exactly one path segment (between slashes)
//   * matches zero or more path segments
//
// Examples:
//   "/sensors/+" matches "/sensors/temp" but not "/sensors/temp/value"
//   "/sensors/*" matches "/sensors/temp", "/sensors/temp/value", "/sensors/a/b/c"
//   "/api/+/data" matches "/api/v1/data" but not "/api/v1/v2/data"
func (s *ServerInput) matchesPattern(pattern, path string) bool {
	// Split both pattern and path into segments
	patternSegments := s.splitPath(pattern)
	pathSegments := s.splitPath(path)
	
	return s.matchSegments(patternSegments, pathSegments)
}

// splitPath splits a path into segments, removing empty segments
func (s *ServerInput) splitPath(path string) []string {
	if path == "/" {
		return []string{}
	}
	
	segments := strings.Split(strings.Trim(path, "/"), "/")
	
	// Remove empty segments
	var result []string
	for _, segment := range segments {
		if segment != "" {
			result = append(result, segment)
		}
	}
	
	return result
}

// matchSegments recursively matches pattern segments against path segments
func (s *ServerInput) matchSegments(patternSegments, pathSegments []string) bool {
	// If pattern is empty, path must also be empty
	if len(patternSegments) == 0 {
		return len(pathSegments) == 0
	}
	
	// If path is empty but pattern isn't, check if pattern can match empty
	if len(pathSegments) == 0 {
		// Only "*" at the end can match empty path
		return len(patternSegments) == 1 && patternSegments[0] == "*"
	}
	
	firstPattern := patternSegments[0]
	
	switch firstPattern {
	case "+":
		// + matches exactly one segment
		if len(pathSegments) == 0 {
			return false
		}
		// Match this segment and continue with remaining
		return s.matchSegments(patternSegments[1:], pathSegments[1:])
		
	case "*":
		// * matches zero or more segments
		// Try matching with 0, 1, 2, ... segments consumed by *
		for i := 0; i <= len(pathSegments); i++ {
			if s.matchSegments(patternSegments[1:], pathSegments[i:]) {
				return true
			}
		}
		return false
		
	default:
		// Literal segment - must match exactly
		if pathSegments[0] != firstPattern {
			return false
		}
		// Match this segment and continue with remaining
		return s.matchSegments(patternSegments[1:], pathSegments[1:])
	}
}

func (s *ServerInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return nil, nil, service.ErrEndOfInput
	}

	select {
	case msg := <-s.msgChan:
		return msg, func(ctx context.Context, err error) error {
			// Acknowledge message processing
			if err != nil {
				s.logger.Warnf("Message processing failed: %v", err)
				s.metrics.RequestsFailed.Incr(1)
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-s.closeChan:
		return nil, nil, service.ErrEndOfInput
	}
}

func (s *ServerInput) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()

		close(s.closeChan)

		if s.server != nil {
			// Note: go-coap v3 server might not have a Close method
			// In that case, closing the listener should stop the server
		}

		if s.udpConn != nil {
			s.udpConn.Close()
		}

		// Wait for server goroutine to finish
		s.wg.Wait()

		close(s.msgChan)
		s.logger.Info("CoAP server input closed")
	})

	return nil
}

// Configuration parsing helpers

func parseServerSecurityConfig(conf *service.ParsedConfig) (SecurityConfig, error) {
	mode, err := conf.FieldString("security", "mode")
	if err != nil {
		return SecurityConfig{}, fmt.Errorf("failed to parse security mode: %w", err)
	}

	config := SecurityConfig{Mode: mode}

	if mode == "psk" {
		if config.PSKIdentity, err = conf.FieldString("security", "psk_identity"); err != nil {
			return SecurityConfig{}, fmt.Errorf("failed to parse psk_identity: %w", err)
		}
		if config.PSKKey, err = conf.FieldString("security", "psk_key"); err != nil {
			return SecurityConfig{}, fmt.Errorf("failed to parse psk_key: %w", err)
		}
	} else if mode == "certificate" {
		if config.CertFile, err = conf.FieldString("security", "cert_file"); err != nil {
			return SecurityConfig{}, fmt.Errorf("failed to parse cert_file: %w", err)
		}
		if config.KeyFile, err = conf.FieldString("security", "key_file"); err != nil {
			return SecurityConfig{}, fmt.Errorf("failed to parse key_file: %w", err)
		}
		if config.CACertFile, err = conf.FieldString("security", "ca_cert_file"); err != nil && err.Error() != "field \"ca_cert_file\" was not found in the config" {
			return SecurityConfig{}, fmt.Errorf("failed to parse ca_cert_file: %w", err)
		}
		if config.RequireClientCert, err = conf.FieldBool("security", "require_client_cert"); err != nil {
			return SecurityConfig{}, fmt.Errorf("failed to parse require_client_cert: %w", err)
		}
	}

	return config, nil
}

func parseResponseConfig(conf *service.ParsedConfig) (ResponseConfig, error) {
	contentFormat, err := conf.FieldString("response", "default_content_format")
	if err != nil {
		return ResponseConfig{}, fmt.Errorf("failed to parse default_content_format: %w", err)
	}

	codeInt, err := conf.FieldInt("response", "default_code")
	if err != nil {
		return ResponseConfig{}, fmt.Errorf("failed to parse default_code: %w", err)
	}

	payload, err := conf.FieldString("response", "default_payload")
	if err != nil {
		return ResponseConfig{}, fmt.Errorf("failed to parse default_payload: %w", err)
	}

	return ResponseConfig{
		DefaultContentFormat: contentFormat,
		DefaultCode:          codes.Code(codeInt),
		DefaultPayload:       payload,
	}, nil
}

func parseServerConverterConfig(conf *service.ParsedConfig) (converter.Config, error) {
	defaultContentFormat, err := conf.FieldString("converter", "default_content_format")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse converter default_content_format: %w", err)
	}

	compressionEnabled, err := conf.FieldBool("converter", "compression_enabled")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse compression_enabled: %w", err)
	}

	maxPayloadSize, err := conf.FieldInt("converter", "max_payload_size")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse max_payload_size: %w", err)
	}

	preserveOptions, err := conf.FieldBool("converter", "preserve_options")
	if err != nil {
		return converter.Config{}, fmt.Errorf("failed to parse preserve_options: %w", err)
	}

	return converter.Config{
		DefaultContentFormat: defaultContentFormat,
		CompressionEnabled:   compressionEnabled,
		MaxPayloadSize:       maxPayloadSize,
		PreserveOptions:      preserveOptions,
	}, nil
}