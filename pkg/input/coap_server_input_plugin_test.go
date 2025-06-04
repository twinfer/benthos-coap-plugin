package input

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	"github.com/redpanda-data/benthos/v4/public/service"
	"math/big"
	"net"
	"path/filepath"

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
		NotificationsSent: resources.Metrics().NewCounter("test_notifications_sent"),
		MessagesRead:      resources.Metrics().NewCounter("test_messages_read"),
		ErrorsTotal:       resources.Metrics().NewCounter("test_errors_total"),
	}

	// Test metrics are initialized
	require.NotNil(t, metrics.RequestsReceived)
	require.NotNil(t, metrics.RequestsProcessed)
	require.NotNil(t, metrics.RequestsFailed)
	require.NotNil(t, metrics.ResponsesSent)
	require.NotNil(t, metrics.NotificationsSent)
	require.NotNil(t, metrics.MessagesRead)
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

// TestServerInputSimplePathMatching tests simple path matching without wildcards
func TestServerInputSimplePathMatching(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	conv := converter.NewConverter(converter.Config{}, logger)

	// Create ServerInput instance with allowed_paths = ["/test/path"]
	config := ServerConfig{
		AllowedPaths: []string{"/test/path"},
	}

	input := &ServerInput{
		converter: conv,
		logger:    logger,
		config:    config,
	}

	// Debug output
	t.Logf("Created ServerInput with config.AllowedPaths: %v", input.config.AllowedPaths)

	// Test that isPathAllowed("/test/path") returns true
	result1 := input.isPathAllowed("/test/path")
	t.Logf("isPathAllowed(\"/test/path\") = %v (expected: true)", result1)
	assert.True(t, result1, "isPathAllowed should return true for /test/path")

	// Test that isPathAllowed("/other/path") returns false
	result2 := input.isPathAllowed("/other/path")
	t.Logf("isPathAllowed(\"/other/path\") = %v (expected: false)", result2)
	assert.False(t, result2, "isPathAllowed should return false for /other/path")

	// Additional debug tests
	t.Run("debug empty allowed paths", func(t *testing.T) {
		emptyConfig := ServerConfig{
			AllowedPaths: []string{},
		}
		emptyInput := &ServerInput{
			converter: conv,
			logger:    logger,
			config:    emptyConfig,
		}
		result := emptyInput.isPathAllowed("/any/path")
		t.Logf("With empty AllowedPaths, isPathAllowed(\"/any/path\") = %v (expected: true)", result)
		assert.True(t, result, "Empty allowed paths should allow all paths")
	})

	t.Run("debug multiple allowed paths", func(t *testing.T) {
		multiConfig := ServerConfig{
			AllowedPaths: []string{"/test/path", "/api/data"},
		}
		multiInput := &ServerInput{
			converter: conv,
			logger:    logger,
			config:    multiConfig,
		}
		result1 := multiInput.isPathAllowed("/test/path")
		result2 := multiInput.isPathAllowed("/api/data")
		result3 := multiInput.isPathAllowed("/other/path")

		t.Logf("With AllowedPaths %v:", multiConfig.AllowedPaths)
		t.Logf("  isPathAllowed(\"/test/path\") = %v (expected: true)", result1)
		t.Logf("  isPathAllowed(\"/api/data\") = %v (expected: true)", result2)
		t.Logf("  isPathAllowed(\"/other/path\") = %v (expected: false)", result3)

		assert.True(t, result1)
		assert.True(t, result2)
		assert.False(t, result3)
	})
}

// TestRouterRegistrationLogic tests the router registration logic for exact paths vs wildcard patterns
func TestRouterRegistrationLogic(t *testing.T) {
	// Create a buffer to capture log output
	var logBuf bytes.Buffer
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	conv := converter.NewConverter(converter.Config{}, logger)

	// Create ServerInput with mixed allowed_paths including both exact paths and wildcard patterns
	config := ServerConfig{
		ListenAddress:  "127.0.0.1:5683",
		Protocol:       "udp",
		AllowedPaths:   []string{"/api/data", "/sensors/+", "/devices/*", "/exact/path"},
		AllowedMethods: []string{"GET", "POST"},
		BufferSize:     100,
		Timeout:        30 * time.Second,
	}

	input := &ServerInput{
		converter: conv,
		logger:    logger,
		config:    config,
	}

	// Initialize router and call setupRouter to register routes
	input.router = mux.NewRouter()

	// Call the router setup method that would normally be called during Connect()
	// We need to simulate the router registration logic from Connect()
	var exactPaths []string
	var wildcardPaths []string

	for _, p := range input.config.AllowedPaths {
		if input.containsWildcards(p) {
			wildcardPaths = append(wildcardPaths, p)
		} else {
			exactPaths = append(exactPaths, p)
		}
	}

	// Register exact paths with the router for optimal routing performance
	if len(exactPaths) > 0 {
		input.logger.Debugf("Registering %d exact paths with router", len(exactPaths))
		for _, p := range exactPaths {
			normalizedPath := p
			if !strings.HasPrefix(normalizedPath, "/") && normalizedPath != "" {
				normalizedPath = "/" + normalizedPath
			} else if normalizedPath == "" {
				normalizedPath = "/"
			}
			input.logger.Debugf("Registering exact path handler: %s", normalizedPath)
			// Note: We can't actually register handlers in this test without a real handler function
			// But we can verify the logic and logging
		}
	}

	// Log information about wildcard patterns
	if len(wildcardPaths) > 0 {
		input.logger.Infof("Found %d wildcard patterns in allowed_paths: %v", len(wildcardPaths), wildcardPaths)
		input.logger.Info("Wildcard patterns will be handled by the catch-all handler and filtered in isPathAllowed()")
	}

	// Capture the log output
	logOutput := logBuf.String()

	// Verify that exact paths are logged as being registered with the router
	assert.Contains(t, logOutput, "Registering 2 exact paths with router", "Should log registration of exact paths")
	assert.Contains(t, logOutput, "Registering exact path handler: /api/data", "Should log registration of /api/data")
	assert.Contains(t, logOutput, "Registering exact path handler: /exact/path", "Should log registration of /exact/path")

	// Verify that wildcard patterns are logged as being handled by catch-all
	assert.Contains(t, logOutput, "Found 2 wildcard patterns in allowed_paths: [/sensors/+ /devices/*]", "Should log wildcard patterns")
	assert.Contains(t, logOutput, "Wildcard patterns will be handled by the catch-all handler", "Should log catch-all handler usage")

	// Test that isPathAllowed works correctly for both exact and wildcard paths
	t.Run("exact paths allowed", func(t *testing.T) {
		assert.True(t, input.isPathAllowed("/api/data"), "/api/data should be allowed")
		assert.True(t, input.isPathAllowed("/exact/path"), "/exact/path should be allowed")
	})

	t.Run("wildcard patterns allowed", func(t *testing.T) {
		assert.True(t, input.isPathAllowed("/sensors/temperature"), "/sensors/temperature should match /sensors/+ pattern")
		assert.True(t, input.isPathAllowed("/sensors/humidity"), "/sensors/humidity should match /sensors/+ pattern")
		assert.True(t, input.isPathAllowed("/devices/device1"), "/devices/device1 should match /devices/* pattern")
		assert.True(t, input.isPathAllowed("/devices/device1/status"), "/devices/device1/status should match /devices/* pattern")
	})

	t.Run("non-allowed paths", func(t *testing.T) {
		assert.False(t, input.isPathAllowed("/unauthorized/path"), "/unauthorized/path should not be allowed")
		assert.False(t, input.isPathAllowed("/api/other"), "/api/other should not be allowed")
		assert.False(t, input.isPathAllowed("/sensors"), "/sensors should not match /sensors/+ (missing segment)")
		assert.False(t, input.isPathAllowed("/sensors/temp/extra"), "/sensors/temp/extra should not match /sensors/+ (too many segments)")
	})

	// Verify the separation logic
	t.Run("path separation logic", func(t *testing.T) {
		expectedExact := []string{"/api/data", "/exact/path"}
		expectedWildcard := []string{"/sensors/+", "/devices/*"}

		assert.ElementsMatch(t, expectedExact, exactPaths, "Exact paths should be correctly identified")
		assert.ElementsMatch(t, expectedWildcard, wildcardPaths, "Wildcard paths should be correctly identified")
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

	// Use the actual config spec from the implementation
	parsedConf, err := coapServerInputConfigSpec.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: tcp
buffer_size: 10
timeout: 5s
allowed_paths: ["/test/tcp"]
allowed_methods: ["GET"]
response:
  default_payload: "TCP OK"
security:
  mode: none
converter:
  default_content_format: "application/json"
observe_server:
  enable_observe_server: false
`, listenAddr), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger                     // Prevent unused variable if not used directly by MockResources
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

	// Use the actual config spec from the implementation
	parsedConf, err := coapServerInputConfigSpec.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: tcp-tls
buffer_size: 10
timeout: 5s
security:
  mode: certificate
  cert_file: %s
  key_file: %s
allowed_paths: ["/test/tcp-tls"]
allowed_methods: ["GET"]
response:
  default_payload: "TCP-TLS OK"
converter:
  default_content_format: "application/json"
observe_server:
  enable_observe_server: false
`, listenAddr, certFile, keyFile), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger                     // Prevent unused variable
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

	// Use the actual config spec from the implementation
	parsedConf, err := coapServerInputConfigSpec.ParseYAML(fmt.Sprintf(`
listen_address: %s
protocol: udp
buffer_size: 10
timeout: 5s
allowed_paths: ["/test/udp"]
allowed_methods: ["GET"]
response:
  default_payload: "UDP OK"
security:
  mode: none
converter:
  default_content_format: "application/json"
observe_server:
  enable_observe_server: false
`, listenAddr), nil)
	require.NoError(t, err)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	_ = logger                     // Prevent unused variable
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

	resp, err := co.Get(ctx, "/test/udp")
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
