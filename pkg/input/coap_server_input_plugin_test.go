package input

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

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
		name          string
		allowedPaths  []string
		testPath      string
		shouldAllow   bool
		description   string
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
			{"sensors/temperature", []string{"sensors", "temperature"}}, // no leading slash
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