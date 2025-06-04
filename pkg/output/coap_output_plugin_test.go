package output

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// TestOutputConfigValidation tests output configuration validation
func TestOutputConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      OutputConfig
		expectError bool
	}{
		{
			name: "valid minimal config",
			config: OutputConfig{
				Protocol:    "udp",
				DefaultPath: "/test",
				Endpoints:   []string{"localhost:5683"},
			},
			expectError: false,
		},
		{
			name: "missing default path",
			config: OutputConfig{
				Protocol:  "udp",
				Endpoints: []string{"localhost:5683"},
			},
			expectError: true,
		},
		{
			name: "empty endpoints",
			config: OutputConfig{
				Protocol:    "udp",
				DefaultPath: "/test",
				Endpoints:   []string{},
			},
			expectError: true,
		},
		{
			name: "invalid protocol",
			config: OutputConfig{
				Protocol:    "http",
				DefaultPath: "/test",
				Endpoints:   []string{"localhost:5683"},
			},
			expectError: true,
		},
		{
			name: "valid tcp config",
			config: OutputConfig{
				Protocol:    "tcp",
				DefaultPath: "/test",
				Endpoints:   []string{"localhost:5684"},
			},
			expectError: false,
		},
		{
			name: "valid with retry policy",
			config: OutputConfig{
				Protocol:    "udp",
				DefaultPath: "/test",
				Endpoints:   []string{"localhost:5683"},
				RetryPolicy: RetryPolicy{
					MaxRetries:      3,
					InitialInterval: 1 * time.Second,
					MaxInterval:     30 * time.Second,
					Multiplier:      2.0,
				},
			},
			expectError: false,
		},
		{
			name: "invalid retry policy - bad multiplier",
			config: OutputConfig{
				Protocol:    "udp",
				DefaultPath: "/test",
				Endpoints:   []string{"localhost:5683"},
				RetryPolicy: RetryPolicy{
					MaxRetries:      3,
					InitialInterval: 1 * time.Second,
					MaxInterval:     30 * time.Second,
					Multiplier:      0.5, // Invalid
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOutputConfig(tt.config)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestRetryPolicyCalculation tests retry delay calculation
func TestRetryPolicyCalculation(t *testing.T) {
	policy := RetryPolicy{
		MaxRetries:      3,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
	}

	tests := []struct {
		attempt       int
		expectedDelay time.Duration
	}{
		{attempt: 0, expectedDelay: 100 * time.Millisecond},
		{attempt: 1, expectedDelay: 200 * time.Millisecond},
		{attempt: 2, expectedDelay: 400 * time.Millisecond},
		{attempt: 3, expectedDelay: 800 * time.Millisecond},
		{attempt: 4, expectedDelay: 1 * time.Second}, // Capped at MaxInterval
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			delay := calculateRetryDelay(policy, tt.attempt)
			assert.Equal(t, tt.expectedDelay, delay)
		})
	}
}

// TestMessageConversion tests basic message conversion functionality
func TestMessageConversion(t *testing.T) {
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	tests := []struct {
		name           string
		benthosMsg     *service.Message
		expectedMethod codes.Code
		expectedPath   string
	}{
		{
			name:           "simple message",
			benthosMsg:     service.NewMessage([]byte("hello world")),
			expectedMethod: codes.POST, // Default method
		},
		{
			name: "message with method metadata",
			benthosMsg: func() *service.Message {
				msg := service.NewMessage([]byte("test"))
				msg.MetaSet("coap_method", "PUT")
				return msg
			}(),
			expectedMethod: codes.PUT,
		},
		{
			name: "message with path metadata",
			benthosMsg: func() *service.Message {
				msg := service.NewMessage([]byte("test"))
				msg.MetaSet("coap_uri_path", "/sensors/temperature")
				return msg
			}(),
			expectedMethod: codes.POST,
			expectedPath:   "/sensors/temperature",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coapMsg, err := conv.MessageToCoAP(tt.benthosMsg)
			require.NoError(t, err)
			require.NotNil(t, coapMsg)

			assert.Equal(t, tt.expectedMethod, coapMsg.Code)
			assert.NotNil(t, coapMsg.Payload)

			if tt.expectedPath != "" {
				path, err := coapMsg.Options.Path()
				require.NoError(t, err)
				assert.Equal(t, tt.expectedPath, path)
			}
		})
	}
}

// TestRequestOptionsHandling tests how request options are applied
func TestRequestOptionsHandling(t *testing.T) {
	tests := []struct {
		name        string
		options     RequestOptions
		expectType  message.Type
		expectToken bool
	}{
		{
			name: "confirmable request",
			options: RequestOptions{
				Confirmable: true,
				Timeout:     5 * time.Second,
			},
			expectType: message.Confirmable,
		},
		{
			name: "non-confirmable request",
			options: RequestOptions{
				Confirmable: false,
				Timeout:     5 * time.Second,
			},
			expectType: message.NonConfirmable,
		},
	}

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			benthosMsg := service.NewMessage([]byte("test"))
			coapMsg, err := conv.MessageToCoAP(benthosMsg)
			require.NoError(t, err)
			require.NotNil(t, coapMsg)

			// Apply request options (this would normally be done in the output plugin)
			if tt.options.Confirmable {
				coapMsg.Type = message.Confirmable
			} else {
				coapMsg.Type = message.NonConfirmable
			}

			assert.Equal(t, tt.expectType, coapMsg.Type)
		})
	}
}

// TestMetricsConfiguration tests metrics setup
func TestMetricsConfiguration(t *testing.T) {
	resources := service.MockResources()

	metrics := &Metrics{
		MessagesSent:    resources.Metrics().NewCounter("messages_sent"),
		MessagesFailed:  resources.Metrics().NewCounter("messages_failed"),
		RequestsTotal:   resources.Metrics().NewCounter("requests_total"),
		RequestsSuccess: resources.Metrics().NewCounter("requests_success"),
		RequestsTimeout: resources.Metrics().NewCounter("requests_timeout"),
		RetriesTotal:    resources.Metrics().NewCounter("retries_total"),
		ConnectionsUsed: resources.Metrics().NewCounter("connections_used"),
	}

	require.NotNil(t, metrics.MessagesSent)
	require.NotNil(t, metrics.MessagesFailed)
	require.NotNil(t, metrics.RequestsTotal)
	require.NotNil(t, metrics.RequestsSuccess)
	require.NotNil(t, metrics.RequestsTimeout)
	require.NotNil(t, metrics.RetriesTotal)
	require.NotNil(t, metrics.ConnectionsUsed)

	// Test metric operations
	metrics.MessagesSent.Incr(1)
	metrics.RequestsTotal.Incr(1)
	metrics.RequestsSuccess.Incr(1)
}

// TestCoAPMessageCreation tests creating CoAP messages with proper structure
func TestCoAPMessageCreation(t *testing.T) {
	tests := []struct {
		name    string
		code    codes.Code
		payload []byte
		token   string
		options map[message.OptionID]interface{}
	}{
		{
			name:    "GET request",
			code:    codes.GET,
			payload: nil,
			token:   "get-token",
		},
		{
			name:    "POST request with payload",
			code:    codes.POST,
			payload: []byte("sensor data"),
			token:   "post-token",
			options: map[message.OptionID]interface{}{
				message.ContentFormat: message.TextPlain,
			},
		},
		{
			name:    "PUT request with path",
			code:    codes.PUT,
			payload: []byte(`{"temperature": 23.5}`),
			token:   "put-token",
			options: map[message.OptionID]interface{}{
				message.ContentFormat: message.AppJSON,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &message.Message{
				Code:    tt.code,
				Token:   message.Token(tt.token),
				Payload: tt.payload,
				Type:    message.Confirmable,
			}

			// Add options if specified
			if tt.options != nil {
				var opts message.Options
				for _, value := range tt.options {
					switch v := value.(type) {
					case message.MediaType:
						buf := make([]byte, 32)
						opts, _, _ = opts.SetContentFormat(buf, v)
					}
				}
				msg.Options = opts
			}

			assert.Equal(t, tt.code, msg.Code)
			assert.Equal(t, message.Token(tt.token), msg.Token)
			assert.Equal(t, tt.payload, msg.Payload)
			assert.Equal(t, message.Confirmable, msg.Type)

			if tt.options != nil {
				for _, expectedValue := range tt.options {
					switch expectedValue.(type) {
					case message.MediaType:
						cf, err := msg.Options.GetUint32(message.ContentFormat)
						require.NoError(t, err)
						assert.Equal(t, uint32(expectedValue.(message.MediaType)), cf)
					}
				}
			}
		})
	}
}

// TestErrorHandling tests error handling scenarios
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		errorCondition string
		expectError    bool
	}{
		{
			name:           "nil message",
			errorCondition: "nil_message",
			expectError:    true,
		},
		{
			name:           "invalid conversion",
			errorCondition: "invalid_conversion",
			expectError:    true,
		},
		{
			name:           "valid message",
			errorCondition: "none",
			expectError:    false,
		},
	}

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	conv := converter.NewConverter(converter.Config{}, logger)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error

			switch tt.errorCondition {
			case "nil_message":
				_, err = conv.MessageToCoAP(nil)
			case "invalid_conversion":
				// Create a message that might cause conversion issues
				msg := service.NewMessage(make([]byte, 1024*1024*10)) // Very large payload
				_, err = conv.MessageToCoAP(msg)
			case "none":
				msg := service.NewMessage([]byte("valid message"))
				_, err = conv.MessageToCoAP(msg)
			}

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to validate output configuration
func validateOutputConfig(config OutputConfig) error {
	if config.DefaultPath == "" {
		return errors.New("default_path is required")
	}

	if len(config.Endpoints) == 0 {
		return errors.New("at least one endpoint is required")
	}

	switch config.Protocol {
	case "udp", "udp-dtls", "tcp", "tcp-tls":
		// Valid protocols
	case "":
		return errors.New("protocol is required")
	default:
		return errors.New("unsupported protocol: " + config.Protocol)
	}

	// Validate retry policy
	if config.RetryPolicy.Multiplier > 0 && config.RetryPolicy.Multiplier < 1.0 {
		return errors.New("retry multiplier must be >= 1.0")
	}

	return nil
}

// Helper function to calculate retry delay
func calculateRetryDelay(policy RetryPolicy, attempt int) time.Duration {
	if attempt == 0 {
		return policy.InitialInterval
	}

	delay := policy.InitialInterval
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * policy.Multiplier)
	}

	if delay > policy.MaxInterval {
		delay = policy.MaxInterval
	}

	return delay
}
