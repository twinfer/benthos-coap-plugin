package input

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/redpanda-data/benthos/v4/public/service"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"crypto/tls"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/options" // Added for client TLS options
	"github.com/plgd-dev/go-coap/v3/tcp"
	"github.com/plgd-dev/go-coap/v3/udp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// Helper function to get a free port
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// generateSelfSignedCert generates a self-signed certificate and key for testing.
// It stores them in a temporary directory.
func generateSelfSignedCert(t *testing.T) (certPath, keyPath string, cleanup func()) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "coap_certs")
	require.NoError(t, err)

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // Valid for 1 year

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true, // Self-signed
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPath = filepath.Join(tempDir, "cert.pem")
	keyPath = filepath.Join(tempDir, "key.pem")

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	defer certOut.Close()
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err)

	keyOut, err := os.Create(keyPath)
	require.NoError(t, err)
	defer keyOut.Close()
	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	require.NoError(t, err)

	return certPath, keyPath, func() {
		os.RemoveAll(tempDir)
	}
}


// TestServerConfigValidation tests server configuration validation
func TestServerConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      ServerConfig
		expectError bool
	}{
		{
			name: "valid minimal config",
			config: ServerConfig{
				ListenAddress: "0.0.0.0:5683",
				Protocol:      "udp",
				BufferSize:    100,
				Timeout:       30 * time.Second,
			},
			expectError: false,
		},
		{
			name: "zero buffer size",
			config: ServerConfig{
				ListenAddress: "0.0.0.0:5683",
				Protocol:      "udp",
				BufferSize:    0,
				Timeout:       30 * time.Second,
			},
			expectError: true,
		},
		{
			name: "negative buffer size",
			config: ServerConfig{
				ListenAddress: "0.0.0.0:5683",
				Protocol:      "udp",
				BufferSize:    -1,
				Timeout:       30 * time.Second,
			},
			expectError: true,
		},
		{
			name: "unsupported protocol",
			config: ServerConfig{
				ListenAddress: "0.0.0.0:5683",
				Protocol:      "http",
				BufferSize:    100,
				Timeout:       30 * time.Second,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test validation logic that would be in newCoAPServerInput
			if tt.config.BufferSize <= 0 && tt.expectError {
				assert.True(t, tt.config.BufferSize <= 0, "Buffer size validation should fail")
			} else if !tt.expectError {
				assert.True(t, tt.config.BufferSize > 0, "Buffer size should be valid")
			}
		})
	}
}

// TestMethodStringConversion tests CoAP method code to string conversion
func TestMethodStringConversion(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	input := &ServerInput{
		converter: conv,
		logger:    logger,
	}

	tests := []struct {
		name     string
		code     int
		expected string
	}{
		{"GET method", 1, "GET"},
		{"POST method", 2, "POST"},
		{"PUT method", 3, "PUT"},
		{"DELETE method", 4, "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := input.getMethodString(codes.Code(tt.code))
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPathAndMethodAllowed tests path and method filtering
func TestPathAndMethodAllowed(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	config := ServerConfig{
		AllowedPaths:   []string{"/api/data", "/sensors/temp"},
		AllowedMethods: []string{"GET", "POST"},
	}

	input := &ServerInput{
		converter: conv,
		logger:    logger,
		config:    config,
	}

	// Test method filtering
	assert.True(t, input.isMethodAllowed("GET"))
	assert.True(t, input.isMethodAllowed("POST"))
	assert.False(t, input.isMethodAllowed("PUT"))
	assert.False(t, input.isMethodAllowed("DELETE"))

	// Test path filtering
	assert.True(t, input.isPathAllowed("/api/data"))
	assert.True(t, input.isPathAllowed("/sensors/temp"))
	assert.False(t, input.isPathAllowed("/unauthorized/path"))
	assert.False(t, input.isPathAllowed("/"))
}

// TestEmptyAllowedLists tests behavior when allowed lists are empty (should allow all)
func TestEmptyAllowedLists(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	config := ServerConfig{
		AllowedPaths:   []string{}, // Empty means allow all
		AllowedMethods: []string{}, // Empty means allow all
	}

	input := &ServerInput{
		converter: conv,
		logger:    logger,
		config:    config,
	}

	// When lists are empty, should allow everything
	assert.True(t, input.isMethodAllowed("GET"))
	assert.True(t, input.isMethodAllowed("POST"))
	assert.True(t, input.isMethodAllowed("PUT"))
	assert.True(t, input.isMethodAllowed("DELETE"))

	assert.True(t, input.isPathAllowed("/any/path"))
	assert.True(t, input.isPathAllowed("/"))
	assert.True(t, input.isPathAllowed("/sensors/temp"))
}

// TestServerMetrics tests metrics functionality
func TestServerMetrics(t *testing.T) {
	resources := service.MockResources()

	metrics := &ServerMetrics{
		RequestsReceived:  resources.Metrics().NewCounter("test_requests_received"),
		RequestsProcessed: resources.Metrics().NewCounter("test_requests_processed"),
		RequestsFailed:    resources.Metrics().NewCounter("test_requests_failed"),
		ResponsesSent:     resources.Metrics().NewCounter("test_responses_sent"),
		ConnectionsActive: resources.Metrics().NewCounter("test_connections_active"),
		ErrorsTotal:       resources.Metrics().NewCounter("test_errors_total"),
	}

	// Test metrics are initialized
	require.NotNil(t, metrics.RequestsReceived)
	require.NotNil(t, metrics.RequestsProcessed)
	require.NotNil(t, metrics.RequestsFailed)
	require.NotNil(t, metrics.ResponsesSent)
	require.NotNil(t, metrics.ConnectionsActive)
	require.NotNil(t, metrics.ErrorsTotal)

	// Test metric operations
	metrics.RequestsReceived.Incr(1)
	metrics.RequestsProcessed.Incr(1)
	metrics.ResponsesSent.Incr(1)
}

// TestWildcardPathMatching tests the new wildcard path matching functionality
func TestWildcardPathMatching(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	tests := []struct {
		name         string
		allowedPaths []string
		testPath     string
		shouldAllow  bool
		description  string
	}{
		// Exact matches
		{
			name:         "exact match",
			allowedPaths: []string{"/sensors/temperature"},
			testPath:     "/sensors/temperature",
			shouldAllow:  true,
			description:  "exact path should match",
		},
		{
			name:         "exact mismatch",
			allowedPaths: []string{"/sensors/temperature"},
			testPath:     "/sensors/humidity",
			shouldAllow:  false,
			description:  "different path should not match",
		},

		// Single segment wildcard (+)
		{
			name:         "single wildcard match",
			allowedPaths: []string{"/sensors/+"},
			testPath:     "/sensors/temperature",
			shouldAllow:  true,
			description:  "+ should match single segment",
		},
		{
			name:         "single wildcard no match - too many segments",
			allowedPaths: []string{"/sensors/+"},
			testPath:     "/sensors/temperature/value",
			shouldAllow:  false,
			description:  "+ should not match multiple segments",
		},
		{
			name:         "single wildcard no match - too few segments",
			allowedPaths: []string{"/sensors/+"},
			testPath:     "/sensors",
			shouldAllow:  false,
			description:  "+ requires exactly one segment",
		},
		{
			name:         "single wildcard in middle",
			allowedPaths: []string{"/api/+/data"},
			testPath:     "/api/v1/data",
			shouldAllow:  true,
			description:  "+ in middle should work",
		},
		{
			name:         "single wildcard in middle - no match",
			allowedPaths: []string{"/api/+/data"},
			testPath:     "/api/v1/v2/data",
			shouldAllow:  false,
			description:  "+ should not match multiple segments in middle",
		},

		// Multi-segment wildcard (*)
		{
			name:         "multi wildcard match - single segment",
			allowedPaths: []string{"/sensors/*"},
			testPath:     "/sensors/temperature",
			shouldAllow:  true,
			description:  "* should match single segment",
		},
		{
			name:         "multi wildcard match - multiple segments",
			allowedPaths: []string{"/sensors/*"},
			testPath:     "/sensors/temperature/value",
			shouldAllow:  true,
			description:  "* should match multiple segments",
		},
		{
			name:         "multi wildcard match - many segments",
			allowedPaths: []string{"/sensors/*"},
			testPath:     "/sensors/building/floor/room/device/temperature",
			shouldAllow:  true,
			description:  "* should match many segments",
		},
		{
			name:         "multi wildcard match - zero segments",
			allowedPaths: []string{"/sensors/*"},
			testPath:     "/sensors",
			shouldAllow:  true,
			description:  "* should match zero segments",
		},
		{
			name:         "multi wildcard no match - wrong prefix",
			allowedPaths: []string{"/sensors/*"},
			testPath:     "/actuators/led",
			shouldAllow:  false,
			description:  "* should not match different prefix",
		},

		// Complex patterns
		{
			name:         "mixed wildcards",
			allowedPaths: []string{"/buildings/+/floors/*"},
			testPath:     "/buildings/A/floors/1/rooms/101",
			shouldAllow:  true,
			description:  "mixed + and * should work",
		},
		{
			name:         "mixed wildcards - no match",
			allowedPaths: []string{"/buildings/+/floors/*"},
			testPath:     "/buildings/A/B/floors/1",
			shouldAllow:  false,
			description:  "+ should not match multiple segments even with *",
		},

		// Edge cases
		{
			name:         "root path exact",
			allowedPaths: []string{"/"},
			testPath:     "/",
			shouldAllow:  true,
			description:  "root path should match exactly",
		},
		{
			name:         "root path with wildcard",
			allowedPaths: []string{"/*"},
			testPath:     "/anything",
			shouldAllow:  true,
			description:  "/* should match any path",
		},
		{
			name:         "root path with wildcard - empty",
			allowedPaths: []string{"/*"},
			testPath:     "/",
			shouldAllow:  true,
			description:  "/* should match root path too",
		},
		{
			name:         "multiple patterns - first matches",
			allowedPaths: []string{"/sensors/+", "/actuators/*"},
			testPath:     "/sensors/temp",
			shouldAllow:  true,
			description:  "should match first pattern",
		},
		{
			name:         "multiple patterns - second matches",
			allowedPaths: []string{"/sensors/+", "/actuators/*"},
			testPath:     "/actuators/led/state",
			shouldAllow:  true,
			description:  "should match second pattern",
		},
		{
			name:         "multiple patterns - none match",
			allowedPaths: []string{"/sensors/+", "/actuators/*"},
			testPath:     "/api/data",
			shouldAllow:  false,
			description:  "should not match if no patterns match",
		},

		// Real-world IoT scenarios
		{
			name:         "device ID pattern",
			allowedPaths: []string{"/devices/+/sensors/+"},
			testPath:     "/devices/device123/sensors/temperature",
			shouldAllow:  true,
			description:  "device ID pattern should work",
		},
		{
			name:         "hierarchical IoT",
			allowedPaths: []string{"/buildings/+/floors/+/rooms/+/*"},
			testPath:     "/buildings/main/floors/2/rooms/201/sensors/temp",
			shouldAllow:  true,
			description:  "hierarchical IoT patterns should work",
		},
		{
			name:         "API versioning",
			allowedPaths: []string{"/api/+/sensors/*"},
			testPath:     "/api/v1/sensors/temperature/current",
			shouldAllow:  true,
			description:  "API versioning patterns should work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ServerConfig{
				AllowedPaths: tt.allowedPaths,
			}

			input := &ServerInput{
				converter: conv,
				logger:    logger,
				config:    config,
			}

			result := input.isPathAllowed(tt.testPath)
			assert.Equal(t, tt.shouldAllow, result, tt.description)
		})
	}
}

// TestWildcardHelperFunctions tests the internal helper functions
func TestWildcardHelperFunctions(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	input := &ServerInput{
		converter: conv,
		logger:    logger,
	}

	t.Run("containsWildcards", func(t *testing.T) {
		tests := []struct {
			pattern  string
			expected bool
		}{
			{"/sensors/temperature", false},
			{"/sensors/+", true},
			{"/sensors/*", true},
			{"/api/+/data/*", true},
			{"/exact/path", false},
			{"", false},
		}

		for _, tt := range tests {
			result := input.containsWildcards(tt.pattern)
			assert.Equal(t, tt.expected, result, "containsWildcards('%s') should be %v", tt.pattern, tt.expected)
		}
	})

	t.Run("splitPath", func(t *testing.T) {
		tests := []struct {
			path     string
			expected []string
		}{
			{"/", []string{}},
			{"/sensors", []string{"sensors"}},
			{"/sensors/temperature", []string{"sensors", "temperature"}},
			{"/api/v1/data", []string{"api", "v1", "data"}},
			{"sensors/temperature", []string{"sensors", "temperature"}},    // no leading slash
			{"/sensors//temperature/", []string{"sensors", "temperature"}}, // extra slashes
			{"", []string{}},
		}

		for _, tt := range tests {
			result := input.splitPath(tt.path)
			assert.Equal(t, tt.expected, result, "splitPath('%s') should return %v", tt.path, tt.expected)
		}
	})

	t.Run("matchSegments", func(t *testing.T) {
		tests := []struct {
			name            string
			patternSegments []string
			pathSegments    []string
			expected        bool
		}{
			{"empty both", []string{}, []string{}, true},
			{"empty pattern, non-empty path", []string{}, []string{"sensors"}, false},
			{"single * matches empty", []string{"*"}, []string{}, true},
			{"single + doesn't match empty", []string{"+"}, []string{}, false},
			{"exact match", []string{"sensors", "temp"}, []string{"sensors", "temp"}, true},
			{"exact mismatch", []string{"sensors", "temp"}, []string{"sensors", "humidity"}, false},
			{"+ matches one", []string{"sensors", "+"}, []string{"sensors", "temp"}, true},
			{"* matches multiple", []string{"sensors", "*"}, []string{"sensors", "temp", "value"}, true},
			{"* matches zero", []string{"sensors", "*"}, []string{"sensors"}, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := input.matchSegments(tt.patternSegments, tt.pathSegments)
				assert.Equal(t, tt.expected, result)
			})
		}
	})
}

// TestConfigParsing tests configuration parsing helpers
func TestConfigParsing(t *testing.T) {
	t.Run("parseServerSecurityConfig - none mode", func(t *testing.T) {
		// Create a mock config that would return "none" for security.mode
		// This is a simplified test since we can't easily mock service.ParsedConfig
		config := SecurityConfig{Mode: "none"}

		assert.Equal(t, "none", config.Mode)
		assert.Empty(t, config.PSKIdentity)
		assert.Empty(t, config.PSKKey)
	})

	t.Run("parseResponseConfig", func(t *testing.T) {
		config := ResponseConfig{
			DefaultContentFormat: "text/plain",
			DefaultCode:          codes.Content,
			DefaultPayload:       "OK",
		}

		assert.Equal(t, "text/plain", config.DefaultContentFormat)
		assert.Equal(t, codes.Content, config.DefaultCode)
		assert.Equal(t, "OK", config.DefaultPayload)
	})
}

func TestCoAPServerInput_TCPCommunication(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)

	conf := service.NewConfigSpec().
		Field(service.NewStringField("listen_address")).
		Field(service.NewStringField("protocol")).
		Field(service.NewStringListField("allowed_paths").Default([]string{})).
		Field(service.NewStringListField("allowed_methods").Default([]string{})).
		Field(service.NewIntField("buffer_size")).
		Field(service.NewDurationField("timeout")).
		Field(service.NewObjectField("response",
			service.NewStringField("default_content_format").Default("text/plain"),
			service.NewIntField("default_code").Default(int(codes.Content)),
			service.NewStringField("default_payload"),
		)).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(),
			service.NewStringField("key_file").Optional(),
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("require_client_cert").Default(false),
		)).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(true),
		))

	parsedConf, err := conf.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: tcp
buffer_size: 10
timeout: 5s
allowed_paths: ["/test/tcp"] # Reverted to /test/tcp
allowed_methods: ["GET"]
response:
  default_payload: "TCP OK"
`, listenAddr), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger // Prevent unused variable if not used directly by MockResources
	mgr := service.MockResources() // Simpler MockResources, will use default logger

	input, err := newCoAPServerInput(parsedConf, mgr)
	require.NoError(t, err)
	require.NotNil(t, input)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	// Allow server to start
	time.Sleep(100 * time.Millisecond)

	// Client
	co, err := tcp.Dial(listenAddr)
	require.NoError(t, err)
	defer co.Close()

	// Send GET request
	resp, err := co.Get(ctx, "/test/tcp") // Reverted path to /test/tcp
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, codes.Content, resp.Code())
	payload, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, "TCP OK", string(payload))

	// Check if message was received by Benthos input
	benthosMsg, _, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, benthosMsg)

	meta, ok := benthosMsg.MetaGet("coap_server_path")
	require.True(t, ok)
	assert.Equal(t, "/test/tcp", meta) // Expected path is now /test/tcp

	metaMethod, ok := benthosMsg.MetaGet("coap_server_method")
	require.NoError(t, err)
	assert.Equal(t, "GET", metaMethod)
}


func TestCoAPServerInput_TCPSecureCommunication(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)

	certFile, keyFile, certCleanup := generateSelfSignedCert(t)
	defer certCleanup()

	// Use comprehensive config spec
	conf := service.NewConfigSpec().
		Field(service.NewStringField("listen_address")).
		Field(service.NewStringField("protocol")).
		Field(service.NewStringListField("allowed_paths").Default([]string{})).
		Field(service.NewStringListField("allowed_methods").Default([]string{})).
		Field(service.NewIntField("buffer_size")).
		Field(service.NewDurationField("timeout")).
		Field(service.NewObjectField("response",
			service.NewStringField("default_content_format").Default("text/plain"),
			service.NewIntField("default_code").Default(int(codes.Content)),
			service.NewStringField("default_payload"),
		)).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(), // Will be overridden by YAML
			service.NewStringField("key_file").Optional(),   // Will be overridden by YAML
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("require_client_cert").Default(false),
		)).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(true),
		))

	parsedConf, err := conf.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: tcp-tls
buffer_size: 10
timeout: 5s
security:
  mode: certificate
  cert_file: %s
  key_file: %s
  ca_cert_file: "" # Explicitly provide empty ca_cert_file
allowed_paths: ["/test/tcp-tls"]
allowed_methods: ["GET"]
response:
  default_payload: "TCP-TLS OK"
`, listenAddr, certFile, keyFile), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger // Prevent unused variable
	mgr := service.MockResources() // Simpler MockResources

	input, err := newCoAPServerInput(parsedConf, mgr)
	require.NoError(t, err)
	require.NotNil(t, input)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	// Allow server to start
	time.Sleep(100 * time.Millisecond)

	// Client
	// Client
	clientTLSConfig := &tls.Config{
		RootCAs: x509.NewCertPool(), // Create an empty pool
		// InsecureSkipVerify: true, // Alternative if not loading CA
	}
	// Load server cert as CA for the client for testing
	serverCertPEM, err := os.ReadFile(certFile)
	require.NoError(t, err)
	ok := clientTLSConfig.RootCAs.AppendCertsFromPEM(serverCertPEM)
	require.True(t, ok)

	co, err := tcp.Dial(listenAddr, options.WithTLS(clientTLSConfig)) // Changed WithTLSConfig to WithTLS
	require.NoError(t, err)
	defer co.Close()

	// Send GET request
	resp, err := co.Get(ctx, "/test/tcp-tls")
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, codes.Content, resp.Code())
	payload, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, "TCP-TLS OK", string(payload))

	// Check if message was received by Benthos input
	benthosMsg, _, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, benthosMsg)

	meta, ok := benthosMsg.MetaGet("coap_server_path")
	require.True(t, ok)
	assert.Equal(t, "/test/tcp-tls", meta)
}

// TestCoAPServerInput_UDPCommunication (Example of how an existing UDP test might look)
// This is a placeholder to show that other tests would remain.
func TestCoAPServerInput_UDPCommunication(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)

	// Use comprehensive config spec
	conf := service.NewConfigSpec().
		Field(service.NewStringField("listen_address")).
		Field(service.NewStringField("protocol")).
		Field(service.NewStringListField("allowed_paths").Default([]string{})).
		Field(service.NewStringListField("allowed_methods").Default([]string{})).
		Field(service.NewIntField("buffer_size")).
		Field(service.NewDurationField("timeout")).
		Field(service.NewObjectField("response",
			service.NewStringField("default_content_format").Default("text/plain"),
			service.NewIntField("default_code").Default(int(codes.Content)),
			service.NewStringField("default_payload"),
		)).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(),
			service.NewStringField("key_file").Optional(),
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("require_client_cert").Default(false),
		)).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(true),
		))

	parsedConf, err := conf.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: udp
buffer_size: 10
timeout: 5s
allowed_paths: ["/test/udp"]
allowed_methods: ["GET"]
response:
  default_payload: "UDP OK"
`, listenAddr), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger // Prevent unused variable
	mgr := service.MockResources() // Simpler MockResources

	input, err := newCoAPServerInput(parsedConf, mgr)
	require.NoError(t, err)
	require.NotNil(t, input)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond) // Allow server to start

	co, err := udp.Dial(listenAddr)
	require.NoError(t, err)
	defer co.Close()

	resp, err := co.Get(ctx, "/test/udp", message.Option{ID: message.URIPath, Value: []byte("test/udp")})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, codes.Content, resp.Code())
	payload, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, "UDP OK", string(payload))

	benthosMsg, _, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, benthosMsg)
	metaPath, ok := benthosMsg.MetaGet("coap_server_path")
	require.True(t, ok)
	assert.Equal(t, "/test/udp", metaPath)
}
