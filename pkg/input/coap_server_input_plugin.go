// pkg/input/coap_server_input_plugin.go
package input

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/options"
	dtlsserver "github.com/plgd-dev/go-coap/v3/dtls/server"
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"
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
	EnableObserveServer        bool          `yaml:"enable_observe_server"`
	DefaultNotificationMaxAge time.Duration `yaml:"default_notification_max_age"`
}

// ServerConfig holds all configuration for the CoAP server input.
type ServerConfig struct {
	ListenAddress   string
	Protocol        string
	AllowedPaths    []string
	AllowedMethods  []string
	Security        SecurityConfig
	BufferSize      int
	Timeout         time.Duration
	Response        ResponseConfig
	Converter       benthosConverter.Config
	Observe         ObserveServerConfig
}

// ClientObservation stores information about an observing client.
type ClientObservation struct {
	RemoteAddr      string
	Token           message.Token
	Path            string
	LastSequenceNum uint32
	TCPConn         *tcpClient.Conn
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

	observations map[string]map[string]*ClientObservation
	observeMu    sync.RWMutex
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

	if serverConfig.ListenAddress, err = conf.FieldString("listen_address"); err != nil { return nil, err }
	if serverConfig.Protocol, err = conf.FieldString("protocol"); err != nil { return nil, err }
	if serverConfig.AllowedPaths, err = conf.FieldStringList("allowed_paths"); err != nil { return nil, err }
	if serverConfig.AllowedMethods, err = conf.FieldStringList("allowed_methods"); err != nil { return nil, err }

	// Security Config
	secConf := conf.Namespace("security") // Use Namespace for optional objects
	// For fields within an optional object, FieldString etc. will use default from spec if field OR parent object is missing.
	// Error is only for malformed values if present.
	if serverConfig.Security.Mode, err = secConf.FieldString("mode"); err != nil { return nil, fmt.Errorf("parsing security.mode: %w", err) }
	if serverConfig.Security.PSKIdentity, err = secConf.FieldString("psk_identity"); err != nil { return nil, fmt.Errorf("parsing security.psk_identity: %w", err) }
	if serverConfig.Security.PSKKey, err = secConf.FieldString("psk_key"); err != nil { return nil, fmt.Errorf("parsing security.psk_key: %w", err) }
	if serverConfig.Security.CertFile, err = secConf.FieldString("cert_file"); err != nil { return nil, fmt.Errorf("parsing security.cert_file: %w", err) }
	if serverConfig.Security.KeyFile, err = secConf.FieldString("key_file"); err != nil { return nil, fmt.Errorf("parsing security.key_file: %w", err) }
	if serverConfig.Security.CACertFile, err = secConf.FieldString("ca_cert_file"); err != nil { return nil, fmt.Errorf("parsing security.ca_cert_file: %w", err) }
	if serverConfig.Security.RequireClientCert, err = secConf.FieldBool("require_client_cert"); err != nil { return nil, fmt.Errorf("parsing security.require_client_cert: %w", err) }
	// If "security" namespace was missing, the fields above got their defaults from the spec.
	// Ensure Mode is "none" if it resolved to empty string (e.g. if default wasn't specified and block missing)
	if serverConfig.Security.Mode == "" { serverConfig.Security.Mode = "none" }


	if serverConfig.BufferSize, err = conf.FieldInt("buffer_size"); err != nil { return nil, err }
	if serverConfig.Timeout, err = conf.FieldDuration("timeout"); err != nil { return nil, err }

	respConf := conf.Namespace("response")
	if serverConfig.Response.DefaultContentFormat, err = respConf.FieldString("default_content_format"); err != nil { return nil, err }
	respCodeInt, err := respConf.FieldInt("default_code")
	if err != nil { return nil, err }
	serverConfig.Response.DefaultCode = codes.Code(respCodeInt)
	if serverConfig.Response.DefaultPayload, err = respConf.FieldString("default_payload"); err != nil { return nil, err }

	convConf := conf.Namespace("converter")
	if serverConfig.Converter.DefaultContentFormat, err = convConf.FieldString("default_content_format"); err != nil { return nil, err }
	if serverConfig.Converter.CompressionEnabled, err = convConf.FieldBool("compression_enabled"); err != nil { return nil, err }
	if serverConfig.Converter.MaxPayloadSize, err = convConf.FieldInt("max_payload_size"); err != nil { return nil, err }
	if serverConfig.Converter.PreserveOptions, err = convConf.FieldBool("preserve_options"); err != nil { return nil, err }

	obsConf := conf.Namespace("observe_server")
	if serverConfig.Observe.EnableObserveServer, err = obsConf.FieldBool("enable_observe_server"); err != nil { return nil, err }
	if serverConfig.Observe.DefaultNotificationMaxAge, err = obsConf.FieldDuration("default_notification_max_age"); err != nil { return nil, err }

	if serverConfig.BufferSize <= 0 { return nil, fmt.Errorf("buffer_size must be > 0") }

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
		config:       serverConfig,
		logger:       logger,
		metrics:      metrics,
		converter:    converter,
		msgChan:      make(chan *service.Message, serverConfig.BufferSize),
		closeChan:    make(chan struct{}),
		observations: make(map[string]map[string]*ClientObservation),
	}, nil
}

func (s *ServerInput) isClosed() bool {
	s.mu.RLock(); defer s.mu.RUnlock()
	return s.closed
}

func (s *ServerInput) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.udpServer != nil || s.tcpServer != nil || s.dtlsServer != nil || s.closed {
		s.mu.Unlock(); s.logger.Debug("CoAP server already active or closed."); return nil
	}
	s.mu.Unlock()

	router := mux.NewRouter()
	router.HandleFunc("/*", s.handleRequest)

	// Note: options.Option is the general client/server option type in go-coap v3.
	// Specific server packages might have their own option types or expect these generic ones.
	// options.WithMux is a common one.
	// options.WithBlockwise(enable bool, szx blockwise.SZX, transferTimeout time.Duration) Option
	serverCommonOpts := []options.Option{
		options.WithMux(router),
		options.WithBlockwise(true, message.Szx1024, time.Second*3), // Example: Enable blockwise, SZX 1024, 3s timeout
		options.WithErrors(func(err error) {
			if !s.isClosed() { s.logger.Errorf("CoAP server internal error: %v", err); s.metrics.ErrorsTotal.Incr(1) }
		}),
	}

	var err error
	protocol := strings.ToLower(s.config.Protocol)
	s.logger.Infof("Starting CoAP server on %s proto %s", s.config.ListenAddress, protocol)

	switch protocol {
	case "udp":
		s.mu.Lock(); s.udpServer = udpserver.New(serverCommonOpts...); s.mu.Unlock()
		s.udpConn, err = coapNet.NewListenUDP("udp", s.config.ListenAddress)
		if err != nil { return fmt.Errorf("UDP listen: %w", err) }
		s.wg.Add(1); go func() { defer s.wg.Done(); defer s.udpConn.Close()
			s.logger.Infof("CoAP UDP listening on %s", s.udpConn.LocalAddr())
			if sErr := s.udpServer.Serve(s.udpConn); sErr!=nil && !s.isClosed(){ s.logger.Errorf("UDP Serve: %v",sErr)}
		}()
	case "tcp":
		s.mu.Lock(); s.tcpServer = tcpserver.New(serverCommonOpts...); s.mu.Unlock()
		s.tcpListener, err = coapNet.NewTCPListener("tcp", s.config.ListenAddress)
		if err != nil { return fmt.Errorf("TCP listen: %w", err) }
		s.wg.Add(1); go func() { defer s.wg.Done(); defer s.tcpListener.Close()
			if nl, ok := s.tcpListener.(net.Listener); ok { s.logger.Infof("CoAP TCP listening on %s", nl.Addr())
			} else { s.logger.Info("CoAP TCP listening") } // Should always be net.Listener
			if sErr := s.tcpServer.Serve(s.tcpListener); sErr!=nil && !s.isClosed(){ s.logger.Errorf("TCP Serve: %v",sErr)}
		}()
	case "udp-dtls":
		if s.config.Security.Mode == "none" { return fmt.Errorf("DTLS needs psk/cert security") }
		dtlsConf, cErr := s.createDTLSConfig(); if cErr != nil { return fmt.Errorf("DTLS config: %w", cErr) }
		// dtlsserver.New takes dtlsserver.Option, not generic options.Option
		// We need to wrap generic options or find DTLS-specific ways to set Mux, etc.
		// For now, let's assume common options like WithMux can be adapted or are available.
		// The plgd-dev/go-coap/v3/dtls/server.Server uses options/config.Common, so options.WithMux should work.
		s.mu.Lock(); s.dtlsServer = dtlsserver.New(serverCommonOpts...); s.mu.Unlock()
		s.dtlsListener, err = coapNet.NewDTLSListener("udp", s.config.ListenAddress, dtlsConf)
		if err != nil { return fmt.Errorf("DTLS listen: %w", err) }
		s.wg.Add(1); go func() { defer s.wg.Done(); defer s.dtlsListener.Close()
			// coapNet.DTLSListener is a net.Listener
			s.logger.Infof("CoAP DTLS listening on %s", s.dtlsListener.Addr())
			if sErr := s.dtlsServer.Serve(s.dtlsListener); sErr!=nil && !s.isClosed(){ s.logger.Errorf("DTLS Serve: %v",sErr)}
		}()
	case "tcp-tls":
		if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" { return fmt.Errorf("TCP-TLS needs cert/key") }
		tlsConf, cErr := s.createTLSConfig(); if cErr != nil { return fmt.Errorf("TLS config: %w", cErr) }
		s.mu.Lock(); s.tcpServer = tcpserver.New(serverCommonOpts...); s.mu.Unlock()
		s.tcpListener, err = coapNet.NewTLSListener("tcp", s.config.ListenAddress, tlsConf)
		if err != nil { return fmt.Errorf("TCP-TLS listen: %w", err) }
		s.wg.Add(1); go func() { defer s.wg.Done(); defer s.tcpListener.Close()
			// coapNet.TLSListener wraps a net.Listener, so Addr() should be available.
			s.logger.Infof("CoAP TCP-TLS listening on %s", s.tcpListener.Addr())
			if sErr := s.tcpServer.Serve(s.tcpListener); sErr!=nil && !s.isClosed(){ s.logger.Errorf("TCP-TLS Serve: %v",sErr)}
		}()
	default: return fmt.Errorf("unknown protocol: %s", protocol)
	}
	s.logger.Infof("CoAP server started: %s (%s)", s.config.ListenAddress, protocol); return nil
}

func (s *ServerInput) createTLSConfig() (*tls.Config, error) {
	if s.config.Security.CertFile == "" || s.config.Security.KeyFile == "" {
		return nil, fmt.Errorf("cert_file and key_file must be provided for TLS")
	}
	cert, err := tls.LoadX509KeyPair(s.config.Security.CertFile, s.config.Security.KeyFile)
	if err != nil { return nil, fmt.Errorf("load server cert/key: %w", err) }
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if s.config.Security.CACertFile != "" {
		caCert, err := os.ReadFile(s.config.Security.CACertFile)
		if err != nil { return nil, fmt.Errorf("read CA cert: %w", err) }
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) { return nil, fmt.Errorf("parse CA cert") }
		tlsConfig.ClientCAs = caCertPool
		if s.config.Security.RequireClientCert { tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else { tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven }
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
		if err != nil { return nil, fmt.Errorf("load server cert/key for DTLS: %w", err) }
		dtlsConfig.Certificates = []tls.Certificate{cert}
		if s.config.Security.CACertFile != "" {
			caCert, err := os.ReadFile(s.config.Security.CACertFile)
			if err != nil { return nil, fmt.Errorf("read CA cert for DTLS: %w", err) }
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) { return nil, fmt.Errorf("parse CA cert for DTLS") }
			dtlsConfig.ClientCAs = caCertPool
			if s.config.Security.RequireClientCert { dtlsConfig.ClientAuth = piondtls.RequireAndVerifyClientCert
			} else { dtlsConfig.ClientAuth = piondtls.VerifyClientCertIfGiven }
		}
	default:
		return nil, fmt.Errorf("security mode '%s' unsupported for DTLS", s.config.Security.Mode)
	}
	return dtlsConfig, nil
}

func (s *ServerInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if s.isClosed() { return nil, nil, service.ErrEndOfInput }
	select {
	case msg, open := <-s.msgChan:
		if !open { return nil, nil, service.ErrEndOfInput }
		s.metrics.MessagesRead.Incr(1)
		return msg, func(rctx context.Context, res error) error {
			if res != nil { s.logger.Debugf("NACK received for message: %v", res) }
			return nil
		}, nil
	case <-s.closeChan: return nil, nil, service.ErrEndOfInput
	case <-ctx.Done(): return nil, nil, ctx.Err()
	}
}

func (s *ServerInput) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		if s.closed { s.mu.Unlock(); return }
		s.closed = true
		s.mu.Unlock()
		s.logger.Info("Closing CoAP server input...")
		close(s.closeChan)
		if s.udpConn != nil { if err := s.udpConn.Close(); err != nil { s.logger.Errorf("Close UDP conn: %v", err) } }
		if s.dtlsListener != nil { if err := s.dtlsListener.Close(); err != nil { s.logger.Errorf("Close DTLS listener: %v", err) } }
		if s.tcpListener != nil { if err := s.tcpListener.Close(); err != nil { s.logger.Errorf("Close TCP listener: %v", err) } }
		s.logger.Debug("Waiting for server goroutines...")
		s.wg.Wait()
		s.mu.Lock()
		if s.msgChan != nil { close(s.msgChan) }
		s.mu.Unlock()
		s.logger.Info("CoAP server input closed.")
	})
	return nil
}

func getClientObservationKey(remoteAddr net.Addr, token message.Token) string { return remoteAddr.String() + "#" + hex.EncodeToString(token) }
func (s *ServerInput) getMethodString(code codes.Code) string { /* ... full implementation ... */
	switch code {
	case codes.GET: return "GET"
	case codes.POST: return "POST"
	case codes.PUT: return "PUT"
	case codes.DELETE: return "DELETE"
	default: return code.String()
	}
}
func (s *ServerInput) sendSuccessResponse(w mux.ResponseWriter) { /* ... full implementation using w.SetResponse ... */
	payloadReader := bytes.NewReader([]byte(s.config.Response.DefaultPayload))
	contentType := message.TextPlain
	if s.config.Response.DefaultContentFormat != "" {
		if parsedCt, err := message.ParseMediaType(s.config.Response.DefaultContentFormat); err == nil {
			contentType = parsedCt
		} else {
			s.logger.Warnf("Parse default_content_format '%s' for success: %v. Defaulting to TextPlain.", s.config.Response.DefaultContentFormat, err)
		}
	}
	if err := w.SetResponse(s.config.Response.DefaultCode, contentType, payloadReader); err != nil && !s.isClosed() {
		s.logger.Errorf("Send success response: %v", err); s.metrics.ErrorsTotal.Incr(1)
	}
}
func (s *ServerInput) sendErrorResponse(w mux.ResponseWriter, code codes.Code, errorMsg string) { /* ... full implementation using w.SetResponse ... */
	payloadReader := bytes.NewReader([]byte(errorMsg))
	if err := w.SetResponse(code, message.TextPlain, payloadReader); err != nil && !s.isClosed() {
		s.logger.Errorf("Send error response (code %s): %v", code.String(), err); s.metrics.ErrorsTotal.Incr(1)
	}
}
func (s *ServerInput) splitPath(path string) []string { /* ... full implementation ... */
	trimmedPath := strings.Trim(path, "/"); if trimmedPath == "" { return []string{} }
	segments := strings.Split(trimmedPath, "/"); var result []string
	for _, segment := range segments { if segment != "" { result = append(result, segment) } }
	return result
 }
func (s *ServerInput) containsWildcards(pattern string) bool { return strings.Contains(pattern, "+") || strings.Contains(pattern, "*") }
func (s *ServerInput) matchSegments(pSegs, pathSegs []string) bool { /* ... full implementation ... */
	if len(pSegs) == 0 { return len(pathSegs) == 0 }
	if len(pathSegs) == 0 {
		for _, ps := range pSegs { if ps != "*" { return false } }
		return true
	}
	firstPattern := pSegs[0]
	switch firstPattern {
	case "+": return len(pathSegs) > 0 && s.matchSegments(pSegs[1:], pathSegs[1:]) // Ensure pathSegs is not empty
	case "*":
		if s.matchSegments(pSegs[1:], pathSegs) { return true }
		return len(pathSegs) > 0 && s.matchSegments(pSegs, pathSegs[1:])
	default: return len(pathSegs) > 0 && pSegs[0] == pathSegs[0] && s.matchSegments(pSegs[1:], pathSegs[1:])
	}
}
func (s *ServerInput) isPathAllowed(path string) bool { /* ... full implementation ... */
	if len(s.config.AllowedPaths) == 0 { return true }
	normalizedPath := "/" + strings.Trim(path, "/"); if path == "/" { normalizedPath = "/" }
	pathSegments := s.splitPath(normalizedPath)
	for _, allowedPattern := range s.config.AllowedPaths {
		normalizedPattern := "/" + strings.Trim(allowedPattern, "/"); if allowedPattern == "/" { normalizedPattern = "/" }
		if !s.containsWildcards(normalizedPattern) { if normalizedPattern == normalizedPath { return true }
		} else { if s.matchSegments(s.splitPath(normalizedPattern), pathSegments) { return true } }
	}
	return false
 }
func (s *ServerInput) isMethodAllowed(method string) bool { /* ... full implementation ... */
	if len(s.config.AllowedMethods) == 0 { return true }
	for _, am := range s.config.AllowedMethods { if strings.EqualFold(am, method) { return true } }
	return false
 }
func (s *ServerInput) convertRequestToMessage(poolMsg *pool.Message, method, path, remoteAddr string) (*service.Message, error) { /* ... full implementation ... */
	if poolMsg == nil || poolMsg.Message() == nil { return nil, fmt.Errorf("nil CoAP message") }
	bMsg, err := s.converter.CoAPToMessage(poolMsg.Message())
	if err != nil { return nil, fmt.Errorf("converter: %w", err) }
	bMsg.MetaSet("coap_server_method", method)
	bMsg.MetaSet("coap_server_path", path)
	bMsg.MetaSet("coap_server_protocol", s.config.Protocol)
	bMsg.MetaSet("coap_server_remote_addr", remoteAddr)
	bMsg.MetaSet("coap_server_timestamp", time.Now().Format(time.RFC3339))
	return bMsg, nil
 }
func (s *ServerInput) handleRequest(w mux.ResponseWriter, r *mux.Message) { /* ... full implementation ... */
	s.metrics.RequestsReceived.Incr(1)
	reqPoolMsg := r.Message
	if reqPoolMsg == nil { s.logger.Error("Nil message in handleRequest"); return }
	method := s.getMethodString(reqPoolMsg.Code())
	path, errPath := reqPoolMsg.Options().Path()
	if errPath != nil || path == "" { path = "/"; if errPath != nil { s.logger.Debugf("Path extraction error, default to '/': %v", errPath)}}
	remoteAddrStr := w.Conn().RemoteAddr().String()
	s.logger.Debugf("Req: M=%s P=%s From=%s", method, path, remoteAddrStr)

	if !s.isMethodAllowed(method) { s.logger.Debugf("Disallowed method %s for %s", method, path); s.sendErrorResponse(w, codes.MethodNotAllowed, "Method not allowed"); s.metrics.RequestsFailed.Incr(1); return }
	if !s.isPathAllowed(path) { s.logger.Debugf("Disallowed path %s for %s", path, method); s.sendErrorResponse(w, codes.NotFound, "Path not found"); s.metrics.RequestsFailed.Incr(1); return }

	if s.config.Observe.EnableObserveServer && reqPoolMsg.Code() == codes.GET {
		obsOptVal, observeSet := reqPoolMsg.Options().Observe()
		if observeSet {
			clientKey := getClientObservationKey(w.Conn().RemoteAddr(), reqPoolMsg.Token())
			tokenCopy := make(message.Token, len(reqPoolMsg.Token())); copy(tokenCopy, reqPoolMsg.Token())
			if obsOptVal == 0 {
				s.observeMu.Lock()
				if s.observations[path] == nil { s.observations[path] = make(map[string]*ClientObservation) }
				var tcpConn *tcpClient.Conn
				if s.config.Protocol == "tcp" || s.config.Protocol == "tcp-tls" {
					if cc, ok := w.Conn().(*tcpClient.Conn); ok { tcpConn = cc
					} else { s.logger.Warnf("Could not assert w.Conn() to *tcpClient.Conn for observer %s: %T", clientKey, w.Conn())}
				}
				seq := uint32(0); if obs, exists := s.observations[path][clientKey]; exists { seq = obs.LastSequenceNum }
				s.observations[path][clientKey] = &ClientObservation{RemoteAddr: remoteAddrStr, Token: tokenCopy, Path: path, LastSequenceNum: seq, TCPConn: tcpConn}
				s.observeMu.Unlock()
				s.logger.Infof("Client %s registered for obs on %s", clientKey, path)
				var respOpts message.Options; respOpts = respOpts.SetObserve(seq)
				if s.config.Observe.DefaultNotificationMaxAge > 0 { respOpts = respOpts.SetMaxAge(uint32(s.config.Observe.DefaultNotificationMaxAge.Seconds())) }
				ct := message.TextPlain; if pCt, errCt := message.ParseMediaType(s.config.Response.DefaultContentFormat); errCt == nil { ct = pCt }
				if err := w.SetResponse(s.config.Response.DefaultCode, ct, bytes.NewReader([]byte(s.config.Response.DefaultPayload)), respOpts...); err != nil {
					s.logger.Errorf("Send obs registration response: %v", err)
				} else { s.metrics.ResponsesSent.Incr(1) }
				return
			} else if obsOptVal == 1 {
				s.observeMu.Lock()
				if clientMap, ok := s.observations[path]; ok { delete(clientMap, clientKey); if len(clientMap) == 0 { delete(s.observations, path) } }
				s.observeMu.Unlock()
				s.logger.Infof("Client %s deregistered from obs on %s", clientKey, path)
				s.sendSuccessResponse(w); return
			}
		}
	}
	benthosMsg, convErr := s.convertRequestToMessage(reqPoolMsg, method, path, remoteAddrStr)
	if convErr != nil { s.logger.Errorf("Convert CoAP to Benthos: %v", convErr); s.sendErrorResponse(w, codes.InternalServerError, "Convert request failed"); s.metrics.RequestsFailed.Incr(1); return }
	requestCtx := r.Context(); if requestCtx == nil { requestCtx = context.Background() }
	select {
	case s.msgChan <- benthosMsg:
		s.metrics.RequestsProcessed.Incr(1); s.logger.Debugf("Processed %s %s from %s", method, path, remoteAddrStr)
		s.sendSuccessResponse(w); s.metrics.ResponsesSent.Incr(1)
		if s.config.Observe.EnableObserveServer && (reqPoolMsg.Code() == codes.POST || reqPoolMsg.Code() == codes.PUT || reqPoolMsg.Code() == codes.DELETE) {
			if msgBytes, errBytes := benthosMsg.AsBytes(); errBytes == nil {
				ct := message.TextPlain
				if ctStr := benthosMsg.MetaGetStr("content-type"); ctStr != "" { if pCt, _ := message.ParseMediaType(ctStr); pCt != message.AppOctets { ct = pCt }
				} else if reqCt, ok := reqPoolMsg.Options().ContentFormat(); ok { ct = reqCt }
				go s.sendNotifications(path, msgBytes, ct)
			} else { s.logger.Errorf("Get Benthos msg bytes for notification: %v", errBytes) }
		}
	case <-time.After(s.config.Timeout): s.logger.Warnf("Timeout processing %s %s from %s", method, path, remoteAddrStr); s.sendErrorResponse(w, codes.ServiceUnavailable, "Processing timeout"); s.metrics.RequestsFailed.Incr(1)
	case <-s.closeChan: s.logger.Debugf("Server shutdown, reject %s %s from %s", method, path, remoteAddrStr); s.sendErrorResponse(w, codes.ServiceUnavailable, "Server shutting down")
	case <-requestCtx.Done(): s.logger.Debugf("Client ctx done, reject %s %s from %s: %v", method, path, remoteAddrStr, requestCtx.Err()); s.metrics.RequestsFailed.Incr(1)
	}
}
func (s *ServerInput) sendNotifications(path string, payload []byte, contentFormat message.MediaType) { /* ... full implementation ... */
	if !s.config.Observe.EnableObserveServer { return }
	s.observeMu.RLock(); clientsForPath, pathExists := s.observations[path]
	if !pathExists { s.observeMu.RUnlock(); return }
	keysToNotify := make([]string, 0, len(clientsForPath)); obsToNotify := make([]*ClientObservation, 0, len(clientsForPath))
	for k, o := range clientsForPath { keysToNotify = append(keysToNotify, k); obsToNotify = append(obsToNotify, o) }
	s.observeMu.RUnlock(); s.logger.Debugf("Notifying for path %s to %d observers", path, len(keysToNotify))
	for i, clientKey := range keysToNotify {
		obs := obsToNotify[i]; s.observeMu.Lock(); obs.LastSequenceNum++; currentSeqNum := obs.LastSequenceNum
		tokenCopy := make(message.Token, len(obs.Token)); copy(tokenCopy, obs.Token)
		remoteAddrStr := obs.RemoteAddr; tcpConnRef := obs.TCPConn; s.observeMu.Unlock()
		notificationMsg := pool.NewMessage(context.Background())
		notificationMsg.SetType(message.Confirmable); notificationMsg.SetCode(codes.Content)
		notificationMsg.SetToken(tokenCopy); notificationMsg.SetObserve(currentSeqNum)
		notificationMsg.SetContentFormat(contentFormat)
		if s.config.Observe.DefaultNotificationMaxAge > 0 {
			if errOpt := notificationMsg.SetOption(message.MaxAge, message.EncodeUint32(uint32(s.config.Observe.DefaultNotificationMaxAge.Seconds()))); errOpt != nil {
				s.logger.Warnf("Set MaxAge for notify to %s: %v", clientKey, errOpt)
			}
		}
		notificationMsg.SetBody(bytes.NewReader(payload))
		protocol := strings.ToLower(s.config.Protocol)
		if protocol == "udp" || protocol == "udp-dtls" {
			connForSend := s.udpConn
			if connForSend == nil { s.logger.Errorf("UDP/DTLS conn nil, cannot notify %s", clientKey); pool.ReleaseMessage(notificationMsg); continue }
			udpAddr, err := net.ResolveUDPAddr("udp", remoteAddrStr)
			if err != nil { s.logger.Errorf("Resolve UDP addr %s for %s: %v", remoteAddrStr, clientKey, err); pool.ReleaseMessage(notificationMsg); continue }
			marshaledMsg, errMarshal := message.Marshal(notificationMsg.Message())
			if errMarshal != nil { s.logger.Errorf("Marshal UDP notify for %s: %v", clientKey, errMarshal)
			} else {
				s.logger.Debugf("Sending UDP notify to %s for %s (seq: %d)", udpAddr.String(), path, currentSeqNum)
				if errSend := connForSend.WriteWithContext(context.Background(), udpAddr, marshaledMsg); errSend != nil {
					s.logger.Errorf("Send UDP notify to %s: %v", clientKey, errSend)
				} else { s.metrics.NotificationsSent.Incr(1) }
			}
			pool.ReleaseMessage(notificationMsg)
		} else if protocol == "tcp" || protocol == "tcp-tls" {
			if tcpConnRef != nil {
				s.logger.Debugf("Sending TCP notify to %s for %s (seq: %d)", remoteAddrStr, path, currentSeqNum)
				err := tcpConnRef.WriteMessage(notificationMsg.Message())
				if err != nil { s.logger.Errorf("Send TCP notify to %s: %v", clientKey, err)
				} else { s.metrics.NotificationsSent.Incr(1) }
				pool.ReleaseMessage(notificationMsg)
			} else { s.logger.Warnf("TCPConn nil for observer %s. Cannot notify.", clientKey); pool.ReleaseMessage(notificationMsg) }
		} else { s.logger.Errorf("Unknown proto %s for notify to %s", protocol, clientKey); pool.ReleaseMessage(notificationMsg) }
	}
}
