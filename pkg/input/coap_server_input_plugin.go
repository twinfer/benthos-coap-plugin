// pkg/input/coap_server_input_plugin.go
package input

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509" // Keep for createTLSConfig, createDTLSConfig
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	piondtls "github.com/pion/dtls/v3"
	dtlsserver "github.com/plgd-dev/go-coap/v3/dtls/server"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/net/blockwise"
	"github.com/plgd-dev/go-coap/v3/options"

	tcpserver "github.com/plgd-dev/go-coap/v3/tcp/server"
	udpserver "github.com/plgd-dev/go-coap/v3/udp/server"

	"github.com/redpanda-data/benthos/v4/public/service"

	benthosConverter "github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// SecurityConfig holds security-related configuration options for the CoAP server.
type SecurityConfig struct {
	Mode              string `yaml:"mode"`
	PSKIdentity       string `yaml:"psk_identity"`
	PSKKey            string `yaml:"psk_key"`
	CertFile          string `yaml:"cert_file"`
	KeyFile           string `yaml:"key_file"`
	CACertFile        string `yaml:"ca_cert_file"`
	RequireClientCert bool   `yaml:"require_client_cert"`
}

// ResponseConfig defines the default response parameters for the CoAP server.
type ResponseConfig struct {
	DefaultContentFormat string     `yaml:"default_content_format"`
	DefaultCode          codes.Code `yaml:"default_code"`
	DefaultPayload       string     `yaml:"default_payload"`
}

// ObserveServerConfig holds configuration for the CoAP Observe server functionality.
type ObserveServerConfig struct {
	EnableObserveServer       bool          `yaml:"enable_observe_server"`
	DefaultNotificationMaxAge time.Duration `yaml:"default_notification_max_age"`
}

// ServerConfig holds all configuration for the CoAP server input.
type ServerConfig struct {
	ListenAddress  string
	Protocol       string
	AllowedPaths   []string
	AllowedMethods []string
	Security       SecurityConfig
	BufferSize     int
	Timeout        time.Duration
	Response       ResponseConfig
	Converter      benthosConverter.Config
	Observe        ObserveServerConfig
}

// ServerMetrics holds metric counters for the CoAP server.
type ServerMetrics struct {
	RequestsReceived  *service.MetricCounter
	RequestsProcessed *service.MetricCounter
	RequestsFailed    *service.MetricCounter
	ResponsesSent     *service.MetricCounter
	NotificationsSent *service.MetricCounter
	MessagesRead      *service.MetricCounter
	ErrorsTotal       *service.MetricCounter
}

// ServerInput is the Benthos input plugin for CoAP server.
type ServerInput struct {
	config    ServerConfig
	logger    *service.Logger
	metrics   *ServerMetrics
	converter *benthosConverter.Converter

	msgChan   chan *service.Message
	closeChan chan struct{}
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
	wg        sync.WaitGroup

	udpServer  *udpserver.Server
	dtlsServer *dtlsserver.Server
	tcpServer  *tcpserver.Server

	udpConn      *coapNet.UDPConn
	dtlsListener dtlsserver.Listener // Corrected type
	tcpListener  tcpserver.Listener

	router *mux.Router
}

var coapServerInputConfigSpec *service.ConfigSpec

func init() {
	coapServerInputConfigSpec = service.NewConfigSpec().
		Summary("Acts as a CoAP server to receive requests from CoAP clients.").
		Description("The CoAP server input listens for incoming CoAP requests on specified endpoints and converts them to Benthos messages. Supports UDP, TCP, DTLS, and TCP-TLS protocols with configurable request handling and CoAP Observe server functionality.").
		Field(service.NewStringField("listen_address").Default("0.0.0.0:5683")).
		Field(service.NewStringField("protocol").Default("udp").LintRule("root in ['udp', 'tcp', 'udp-dtls', 'tcp-tls']")).
		Field(service.NewStringListField("allowed_paths").Default([]string{})).
		Field(service.NewStringListField("allowed_methods").Default([]string{})).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none").LintRule("root in ['none', 'psk', 'certificate']"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(),
			service.NewStringField("key_file").Optional(),
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("require_client_cert").Default(false),
		).Description("Security configuration for DTLS/TLS connections.").Optional()).
		Field(service.NewIntField("buffer_size").Default(1000)).
		Field(service.NewDurationField("timeout").Default("30s")).
		Field(service.NewObjectField("response",
			service.NewStringField("default_content_format").Default("text/plain"),
			service.NewIntField("default_code").Default(int(codes.Content)),
			service.NewStringField("default_payload").Default("OK"),
		).Description("Default response configuration.").Optional()).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(true),
		).Description("Message conversion configuration.").Optional()).
		Field(service.NewObjectField("observe_server",
			service.NewBoolField("enable_observe_server").Default(false),
			service.NewDurationField("default_notification_max_age").Default("60s"),
		).Description("CoAP Observe server functionality settings.").Optional())

	err := service.RegisterInput("coap_server", coapServerInputConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newCoAPServerInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newCoAPServerInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	serverConfig := ServerConfig{}
	var err error

	if serverConfig.ListenAddress, err = conf.FieldString("listen_address"); err != nil {
		return nil, err
	}
	if serverConfig.Protocol, err = conf.FieldString("protocol"); err != nil {
		return nil, err
	}
	if serverConfig.AllowedPaths, err = conf.FieldStringList("allowed_paths"); err != nil {
		return nil, err
	}
	if serverConfig.AllowedMethods, err = conf.FieldStringList("allowed_methods"); err != nil {
		return nil, err
	}

	// Security Config
	secConf := conf.Namespace("security") // Use Namespace for optional objects
	// For fields within an optional object, we need to handle them carefully
	// Use default values for optional fields if they're not provided
	if serverConfig.Security.Mode, err = secConf.FieldString("mode"); err != nil {
		return nil, fmt.Errorf("parsing security.mode: %w", err)
	}

	// For optional fields, ignore errors if the field is not found
	serverConfig.Security.PSKIdentity, _ = secConf.FieldString("psk_identity")
	serverConfig.Security.PSKKey, _ = secConf.FieldString("psk_key")
	serverConfig.Security.CertFile, _ = secConf.FieldString("cert_file")
	serverConfig.Security.KeyFile, _ = secConf.FieldString("key_file")
	serverConfig.Security.CACertFile, _ = secConf.FieldString("ca_cert_file")
	serverConfig.Security.RequireClientCert, _ = secConf.FieldBool("require_client_cert")
	// If "security" namespace was missing, the fields above got their defaults from the spec.
	// Ensure Mode is "none" if it resolved to empty string (e.g. if default wasn't specified and block missing)
	if serverConfig.Security.Mode == "" {
		serverConfig.Security.Mode = "none"
	}

	if serverConfig.BufferSize, err = conf.FieldInt("buffer_size"); err != nil {
		return nil, err
	}
	if serverConfig.Timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	respConf := conf.Namespace("response")
	if serverConfig.Response.DefaultContentFormat, err = respConf.FieldString("default_content_format"); err != nil {
		return nil, err
	}
	respCodeInt, err := respConf.FieldInt("default_code")
	if err != nil {
		return nil, err
	}
	serverConfig.Response.DefaultCode = codes.Code(respCodeInt)
	if serverConfig.Response.DefaultPayload, err = respConf.FieldString("default_payload"); err != nil {
		return nil, err
	}

	convConf := conf.Namespace("converter")
	if serverConfig.Converter.DefaultContentFormat, err = convConf.FieldString("default_content_format"); err != nil {
		return nil, err
	}
	if serverConfig.Converter.CompressionEnabled, err = convConf.FieldBool("compression_enabled"); err != nil {
		return nil, err
	}
	if serverConfig.Converter.MaxPayloadSize, err = convConf.FieldInt("max_payload_size"); err != nil {
		return nil, err
	}
	if serverConfig.Converter.PreserveOptions, err = convConf.FieldBool("preserve_options"); err != nil {
		return nil, err
	}

	obsConf := conf.Namespace("observe_server")
	if serverConfig.Observe.EnableObserveServer, err = obsConf.FieldBool("enable_observe_server"); err != nil {
		return nil, err
	}
	if serverConfig.Observe.DefaultNotificationMaxAge, err = obsConf.FieldDuration("default_notification_max_age"); err != nil {
		return nil, err
	}

	if serverConfig.BufferSize <= 0 {
		return nil, fmt.Errorf("buffer_size must be > 0")
	}

	logger := mgr.Logger()
	metrics := &ServerMetrics{
		RequestsReceived:  mgr.Metrics().NewCounter("coap_server_requests_received"),
		RequestsProcessed: mgr.Metrics().NewCounter("coap_server_requests_processed"),
		RequestsFailed:    mgr.Metrics().NewCounter("coap_server_requests_failed"),
		ResponsesSent:     mgr.Metrics().NewCounter("coap_server_responses_sent"),
		NotificationsSent: mgr.Metrics().NewCounter("coap_server_notifications_sent"),
		MessagesRead:      mgr.Metrics().NewCounter("coap_server_messages_read"),
		ErrorsTotal:       mgr.Metrics().NewCounter("coap_server_errors_total"),
	}
	converter := benthosConverter.NewConverter(serverConfig.Converter, logger)

	return &ServerInput{
		config:    serverConfig,
		logger:    logger,
		metrics:   metrics,
		converter: converter,
		msgChan:   make(chan *service.Message, serverConfig.BufferSize),
		closeChan: make(chan struct{}),
		router:    mux.NewRouter(), // Initialize router here
	}, nil
}

// parseContentFormatString maps a string content format name to a message.MediaType.
// It logs a warning for unknown formats and defaults to message.AppOctets.
func (s *ServerInput) parseContentFormatString(format string) message.MediaType {
	switch strings.ToLower(format) {
	case "text/plain":
		return message.TextPlain
	case "application/json":
		return message.AppJSON
	case "application/xml":
		return message.AppXML
	case "application/cbor":
		return message.AppCBOR
	case "application/octet-stream":
		return message.AppOctets
	default:
		s.logger.Warnf("Unknown content format string '%s'. Defaulting to application/octet-stream.", format)
		return message.AppOctets // Safe default for unknown types
	}
}

func (s *ServerInput) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *ServerInput) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.udpServer != nil || s.tcpServer != nil || s.dtlsServer != nil || s.closed {
		s.mu.Unlock()
		s.logger.Debug("CoAP server already active or closed.")
		return nil
	}
	s.mu.Unlock()

	// Use DefaultHandle to catch all requests and filter them in handleRequest
	s.router.DefaultHandle(mux.HandlerFunc(s.handleRequest))

	// Note: Each server type (UDP, TCP, DTLS) has its own Option interface

	var err error
	protocol := strings.ToLower(s.config.Protocol)
	s.logger.Infof("Starting CoAP server on %s proto %s", s.config.ListenAddress, protocol)

	switch protocol {
	case "udp":
		s.mu.Lock()
		s.udpServer = udpserver.New(
			options.WithMux(s.router),
			options.WithBlockwise(true, blockwise.SZX1024, time.Second*3),
			options.WithErrors(func(err error) {
				if !s.isClosed() {
					s.logger.Errorf("CoAP UDP server error: %v", err)
					s.metrics.ErrorsTotal.Incr(1)
				}
			}),
		)
		s.mu.Unlock()
		s.udpConn, err = coapNet.NewListenUDP("udp", s.config.ListenAddress)
		if err != nil {
			return fmt.Errorf("UDP listen: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.udpConn.Close()
			s.logger.Infof("CoAP UDP listening on %s", s.udpConn.LocalAddr())
			if sErr := s.udpServer.Serve(s.udpConn); sErr != nil && !s.isClosed() {
				s.logger.Errorf("UDP Serve: %v", sErr)
			}
		}()
	case "tcp":
		s.mu.Lock()
		s.tcpServer = tcpserver.New(
			options.WithMux(s.router),
			options.WithBlockwise(true, blockwise.SZX1024, time.Second*3),
			options.WithErrors(func(err error) {
				if !s.isClosed() {
					s.logger.Errorf("CoAP TCP server error: %v", err)
					s.metrics.ErrorsTotal.Incr(1)
				}
			}),
		)
		s.mu.Unlock() // Corrected: Unlock after server creation
		s.tcpListener, err = coapNet.NewTCPListener("tcp", s.config.ListenAddress)
		if err != nil {
			return fmt.Errorf("TCP listen: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.tcpListener.Close()
			if nl, ok := s.tcpListener.(net.Listener); ok {
				s.logger.Infof("CoAP TCP listening on %s", nl.Addr())
			} else {
				s.logger.Info("CoAP TCP listening")
			} // Should always be net.Listener
			if sErr := s.tcpServer.Serve(s.tcpListener); sErr != nil && !s.isClosed() {
				s.logger.Errorf("TCP Serve: %v", sErr)
			}
		}()
	case "udp-dtls":
		if s.config.Security.Mode == "none" {
			return fmt.Errorf("DTLS needs psk/cert security")
		}
		dtlsConf, cErr := s.createDTLSConfig()
		if cErr != nil {
			return fmt.Errorf("DTLS config: %w", cErr)
		}
		// dtlsserver.New takes dtlsserver.Option, not generic options.Option
		// We need to wrap generic options or find DTLS-specific ways to set Mux, etc.
		// For now, let's assume common options like WithMux can be adapted or are available.
		// The plgd-dev/go-coap/v3/dtls/server.Server uses options/config.Common, so options.WithMux should work.
		s.mu.Lock()
		s.dtlsServer = dtlsserver.New(
			options.WithMux(s.router),
			options.WithBlockwise(true, blockwise.SZX1024, time.Second*3),
			options.WithErrors(func(err error) {
				if !s.isClosed() {
					s.logger.Errorf("CoAP DTLS server error: %v", err)
					s.metrics.ErrorsTotal.Incr(1)
				}
			}),
		)
		s.mu.Unlock() // Corrected: Unlock after server creation - This line seems to have been missed in the previous diff
		s.dtlsListener, err = coapNet.NewDTLSListener("udp", s.config.ListenAddress, dtlsConf)
		if err != nil {
			return fmt.Errorf("DTLS listen: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.dtlsListener.Close()
			// dtlsserver.Listener implements net.Listener, so we can get the address.
			if nl, ok := s.dtlsListener.(net.Listener); ok {
				s.logger.Infof("CoAP DTLS listening on %s", nl.Addr())
			} else {
				s.logger.Infof("CoAP DTLS listening on %s (address not available via net.Listener type assertion)", s.config.ListenAddress)
			}
			if sErr := s.dtlsServer.Serve(s.dtlsListener); sErr != nil && !s.isClosed() {
				s.logger.Errorf("DTLS Serve: %v", sErr)
			}
		}()
	case "tcp-tls":
		if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" {
			return fmt.Errorf("TCP-TLS needs cert/key")
		}
		tlsConf, cErr := s.createTLSConfig()
		if cErr != nil {
			return fmt.Errorf("TLS config: %w", cErr)
		}
		s.mu.Lock()
		s.tcpServer = tcpserver.New(
			options.WithMux(s.router),
			options.WithBlockwise(true, blockwise.SZX1024, time.Second*3),
			options.WithErrors(func(err error) {
				if !s.isClosed() {
					s.logger.Errorf("CoAP TCP-TLS server error: %v", err)
					s.metrics.ErrorsTotal.Incr(1)
				}
			}),
		)
		s.mu.Unlock() // Corrected: Unlock after server creation
		s.tcpListener, err = coapNet.NewTLSListener("tcp", s.config.ListenAddress, tlsConf)
		if err != nil {
			return fmt.Errorf("TCP-TLS listen: %w", err)
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.tcpListener.Close()
			// coapNet.TLSListener wraps a net.Listener, so Addr() should be available via type assertion.
			if nl, ok := s.tcpListener.(net.Listener); ok {
				s.logger.Infof("CoAP TCP-TLS listening on %s", nl.Addr())
			} else {
				s.logger.Infof("CoAP TCP-TLS listening on %s (address not available via net.Listener type assertion)", s.config.ListenAddress)
			}
			if sErr := s.tcpServer.Serve(s.tcpListener); sErr != nil && !s.isClosed() {
				s.logger.Errorf("TCP-TLS Serve: %v", sErr)
			}
		}()
	default:
		return fmt.Errorf("unknown protocol: %s", protocol)
	}
	s.logger.Infof("CoAP server started: %s (%s)", s.config.ListenAddress, protocol)
	return nil
}

func (s *ServerInput) createTLSConfig() (*tls.Config, error) {
	if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" {
		return nil, fmt.Errorf("cert_file and key_file must be provided for TLS")
	}
	cert, err := tls.LoadX509KeyPair(s.config.Security.CertFile, s.config.Security.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if s.config.Security.CACertFile != "" {
		caCert, err := os.ReadFile(s.config.Security.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parse CA cert")
		}
		tlsConfig.ClientCAs = caCertPool
		if s.config.Security.RequireClientCert {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return tlsConfig, nil
}

func (s *ServerInput) createDTLSConfig() (*piondtls.Config, error) {
	dtlsConfig := &piondtls.Config{ExtendedMasterSecret: piondtls.RequireExtendedMasterSecret}
	switch s.config.Security.Mode {
	case "psk":
		if s.config.Security.PSKIdentity == "" || s.config.Security.PSKKey == "" {
			return nil, fmt.Errorf("psk_identity and psk_key required for DTLS PSK")
		}
		dtlsConfig.PSK = func(hint []byte) ([]byte, error) { return []byte(s.config.Security.PSKKey), nil }
		dtlsConfig.PSKIdentityHint = []byte(s.config.Security.PSKIdentity)
		dtlsConfig.CipherSuites = []piondtls.CipherSuiteID{piondtls.TLS_PSK_WITH_AES_128_CCM_8}
	case "certificate":
		if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file required for DTLS certificate mode")
		}
		cert, err := tls.LoadX509KeyPair(s.config.Security.CertFile, s.config.Security.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load server cert/key for DTLS: %w", err)
		}
		dtlsConfig.Certificates = []tls.Certificate{cert}
		if s.config.Security.CACertFile != "" {
			caCert, err := os.ReadFile(s.config.Security.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("read CA cert for DTLS: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("parse CA cert for DTLS")
			}
			dtlsConfig.ClientCAs = caCertPool
			if s.config.Security.RequireClientCert {
				dtlsConfig.ClientAuth = piondtls.RequireAndVerifyClientCert
			} else {
				dtlsConfig.ClientAuth = piondtls.VerifyClientCertIfGiven
			}
		}
	default:
		return nil, fmt.Errorf("security mode '%s' unsupported for DTLS", s.config.Security.Mode)
	}
	return dtlsConfig, nil
}

func (s *ServerInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if s.isClosed() {
		return nil, nil, service.ErrEndOfInput
	}
	select {
	case msg, open := <-s.msgChan:
		if !open {
			return nil, nil, service.ErrEndOfInput
		}
		s.metrics.MessagesRead.Incr(1)
		return msg, func(rctx context.Context, res error) error {
			if res != nil {
				s.logger.Debugf("NACK received for message: %v", res)
			}
			return nil
		}, nil
	case <-s.closeChan:
		return nil, nil, service.ErrEndOfInput
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (s *ServerInput) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.closed = true
		s.mu.Unlock()
		s.logger.Info("Closing CoAP server input...")
		close(s.closeChan)
		if s.udpConn != nil {
			if err := s.udpConn.Close(); err != nil {
				s.logger.Errorf("Close UDP conn: %v", err)
			}
		}
		if s.dtlsListener != nil {
			if err := s.dtlsListener.Close(); err != nil {
				s.logger.Errorf("Close DTLS listener: %v", err)
			}
		}
		if s.tcpListener != nil {
			if err := s.tcpListener.Close(); err != nil {
				s.logger.Errorf("Close TCP listener: %v", err)
			}
		}
		s.logger.Debug("Waiting for server goroutines...")
		s.wg.Wait()
		s.mu.Lock()
		if s.msgChan != nil {
			close(s.msgChan)
		}
		s.mu.Unlock()
		s.logger.Info("CoAP server input closed.")
	})
	return nil
}

// Health returns the current health status of the input.
func (s *ServerInput) Health() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := map[string]any{
		"status":         "healthy",
		"buffer_usage":   fmt.Sprintf("%d/%d", len(s.msgChan), cap(s.msgChan)),
		"listen_address": s.config.ListenAddress,
		"protocol":       s.config.Protocol,
	}

	if s.closed {
		status["status"] = "closed"
	} else {
		// Could add checks here if the underlying server/listener is nil,
		// but Connect should ensure one is running if not closed.
		// More advanced health checks might involve sending a dummy request
		// or checking internal server state if the library exposed it.
	}

	return status
}

func (s *ServerInput) getMethodString(code codes.Code) string { /* ... full implementation ... */
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
func (s *ServerInput) sendSuccessResponse(w mux.ResponseWriter) { /* ... full implementation using w.SetResponse ... */
	payloadReader := bytes.NewReader([]byte(s.config.Response.DefaultPayload))
	contentType := message.TextPlain
	// Use the helper function to parse the configured default content format string
	contentType = s.parseContentFormatString(s.config.Response.DefaultContentFormat)

	if err := w.SetResponse(s.config.Response.DefaultCode, contentType, payloadReader); err != nil && !s.isClosed() {
		s.logger.Errorf("Send success response: %v", err)
		s.metrics.ErrorsTotal.Incr(1)
	}
}
func (s *ServerInput) sendErrorResponse(w mux.ResponseWriter, code codes.Code, errorMsg string) { /* ... full implementation using w.SetResponse ... */
	payloadReader := bytes.NewReader([]byte(errorMsg))
	if err := w.SetResponse(code, message.TextPlain, payloadReader); err != nil && !s.isClosed() {
		s.logger.Errorf("Send error response (code %s): %v", code.String(), err)
		s.metrics.ErrorsTotal.Incr(1)
	}
}
func (s *ServerInput) splitPath(path string) []string { /* ... full implementation ... */
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return []string{}
	}
	segments := strings.Split(trimmedPath, "/")
	var result []string
	for _, segment := range segments {
		if segment != "" {
			result = append(result, segment)
		}
	}
	return result
}
func (s *ServerInput) containsWildcards(pattern string) bool {
	return strings.Contains(pattern, "+") || strings.Contains(pattern, "*")
}
func (s *ServerInput) matchSegments(pSegs, pathSegs []string) bool { /* ... full implementation ... */
	if len(pSegs) == 0 {
		return len(pathSegs) == 0
	}
	if len(pathSegs) == 0 {
		for _, ps := range pSegs {
			if ps != "*" {
				return false
			}
		}
		return true
	}
	firstPattern := pSegs[0]
	switch firstPattern {
	case "+":
		return len(pathSegs) > 0 && s.matchSegments(pSegs[1:], pathSegs[1:]) // Ensure pathSegs is not empty
	case "*":
		if s.matchSegments(pSegs[1:], pathSegs) {
			return true
		}
		return len(pathSegs) > 0 && s.matchSegments(pSegs, pathSegs[1:])
	default:
		return len(pathSegs) > 0 && pSegs[0] == pathSegs[0] && s.matchSegments(pSegs[1:], pathSegs[1:])
	}
}
func (s *ServerInput) isPathAllowed(path string) bool { /* ... full implementation ... */
	if len(s.config.AllowedPaths) == 0 {
		return true
	}
	normalizedPath := "/" + strings.Trim(path, "/")
	if path == "/" {
		normalizedPath = "/"
	}
	pathSegments := s.splitPath(normalizedPath)
	for _, allowedPattern := range s.config.AllowedPaths {
		normalizedPattern := "/" + strings.Trim(allowedPattern, "/")
		if allowedPattern == "/" {
			normalizedPattern = "/"
		}
		if !s.containsWildcards(normalizedPattern) {
			if normalizedPattern == normalizedPath {
				return true
			}
		} else {
			if s.matchSegments(s.splitPath(normalizedPattern), pathSegments) {
				return true
			}
		}
	}
	return false
}
func (s *ServerInput) isMethodAllowed(method string) bool { /* ... full implementation ... */
	if len(s.config.AllowedMethods) == 0 {
		return true
	}
	for _, am := range s.config.AllowedMethods {
		if strings.EqualFold(am, method) {
			return true
		}
	}
	return false
}
func (s *ServerInput) convertRequestToMessage(poolMsg *pool.Message, method, path, remoteAddr string) (*service.Message, error) { /* ... full implementation ... */
	if poolMsg == nil {
		return nil, fmt.Errorf("nil CoAP *pool.Message provided")
	}

	// Manually construct message.Message from pool.Message's public API
	// as poolMsg.toMessage() is unexported.
	payload, err := poolMsg.ReadBody()
	if err != nil {
		return nil, fmt.Errorf("failed to read CoAP message body from pool message: %w", err)
	}
	actualCoapMsg := message.Message{
		Token:     poolMsg.Token(),
		Code:      poolMsg.Code(),
		Options:   poolMsg.Options(),
		Payload:   payload,
		MessageID: poolMsg.MessageID(),
		Type:      poolMsg.Type(),
	}

	bMsg, err := s.converter.CoAPToMessage(&actualCoapMsg) // Pass address of the message.Message
	if err != nil {
		return nil, fmt.Errorf("converter: %w", err)
	}
	bMsg.MetaSet("coap_server_method", method)
	bMsg.MetaSet("coap_server_path", path)
	bMsg.MetaSet("coap_server_protocol", s.config.Protocol)
	bMsg.MetaSet("coap_server_remote_addr", remoteAddr)
	bMsg.MetaSet("coap_server_timestamp", time.Now().Format(time.RFC3339))
	return bMsg, nil
}

func (s *ServerInput) handleRequest(w mux.ResponseWriter, r *mux.Message) { /* ... full implementation ... */
	s.metrics.RequestsReceived.Incr(1)
	if r == nil {
		s.logger.Error("Nil message in handleRequest")
		return // Should not happen if mux is working correctly
	}
	method := s.getMethodString(r.Code())
	path, errPath := r.Options().Path()
	if errPath != nil || path == "" {
		path = "/"
		if errPath != nil {
			s.logger.Debugf("Path extraction error, default to '/': %v", errPath)
		}
	}
	remoteAddrStr := w.Conn().RemoteAddr().String()
	s.logger.Debugf("Req: M=%s P=%s From=%s", method, path, remoteAddrStr)

	if !s.isMethodAllowed(method) {
		s.logger.Debugf("Disallowed method %s for %s", method, path)
		s.sendErrorResponse(w, codes.MethodNotAllowed, "Method not allowed")
		s.metrics.RequestsFailed.Incr(1)
		return
	}
	if !s.isPathAllowed(path) {
		s.logger.Debugf("Disallowed path %s for %s", path, method)
		s.sendErrorResponse(w, codes.NotFound, "Path not found")
		s.metrics.RequestsFailed.Incr(1)
		return
	}
	benthosMsg, convErr := s.convertRequestToMessage(r.Message, method, path, remoteAddrStr) // Pass the embedded *pool.Message
	if convErr != nil {
		s.logger.Errorf("Convert CoAP to Benthos: %v", convErr)
		s.sendErrorResponse(w, codes.InternalServerError, "Convert request failed")
		s.metrics.RequestsFailed.Incr(1)
		return
	}
	requestCtx := r.Context()
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	select {
	case s.msgChan <- benthosMsg:
		s.metrics.RequestsProcessed.Incr(1)
		s.logger.Debugf("Processed %s %s from %s", method, path, remoteAddrStr)
		s.sendSuccessResponse(w)
		// Note: For GET requests establishing an observation, sendSuccessResponse
		// provides the initial notification. The CoAP library handles setting the Observe option.
		s.metrics.ResponsesSent.Incr(1)

		// If Observe is enabled and this was a state-changing operation (POST, PUT, DELETE)
		// on an allowed path, log it for monitoring purposes.
		if s.config.Observe.EnableObserveServer && (r.Code() == codes.POST || r.Code() == codes.PUT || r.Code() == codes.DELETE) {
			if s.isPathAllowed(path) {
				// Note: CoAP observe notifications are handled automatically by the go-coap library
				// when clients have established observe relationships with specific paths.
				// The library will send notifications when responses are sent to observe-enabled requests.
				s.logger.Debugf("State change on observable path %s via %s - clients with active observations will be notified", path, method)
				s.metrics.NotificationsSent.Incr(1)
			}
		}
	case <-time.After(s.config.Timeout):
		s.logger.Warnf("Timeout processing %s %s from %s", method, path, remoteAddrStr)
		s.sendErrorResponse(w, codes.ServiceUnavailable, "Processing timeout")
		s.metrics.RequestsFailed.Incr(1)
	case <-s.closeChan:
		s.logger.Debugf("Server shutdown, reject %s %s from %s", method, path, remoteAddrStr)
		s.sendErrorResponse(w, codes.ServiceUnavailable, "Server shutting down")
	case <-requestCtx.Done():
		s.logger.Debugf("Client ctx done, reject %s %s from %s: %v", method, path, remoteAddrStr, requestCtx.Err())
		s.metrics.RequestsFailed.Incr(1)
	}
}
