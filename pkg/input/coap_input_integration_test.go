//go:build integration

package input

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockserver "github.com/twinfer/benthos-coap-plugin/pkg/testing" // Our mock server package

	coapMessage "github.com/plgd-dev/go-coap/v3/message"
)

// Helper to create and connect CoapInput, returns the input and a cleanup function.
func setupCoapInput(t *testing.T, confYAML string) (*Input, func()) {
	t.Helper()
	
	// Create the config spec (same as in the plugin)
	configSpec := service.NewConfigSpec().
		Summary("Reads messages from CoAP endpoints using observe subscriptions.").
		Description("The CoAP input establishes observe subscriptions on specified paths and converts incoming CoAP messages to Benthos messages.").
		Field(service.NewStringListField("endpoints").
			Description("List of CoAP endpoints to connect to.")).
		Field(service.NewStringListField("observe_paths").
			Description("List of resource paths to observe for real-time updates.")).
		Field(service.NewStringField("protocol").
			Description("CoAP protocol to use.").
			Default("udp")).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(),
			service.NewStringField("key_file").Optional(),
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("insecure_skip_verify").Default(false)).Optional()).
		Field(service.NewObjectField("connection_pool",
			service.NewIntField("max_size").Default(5),
			service.NewDurationField("idle_timeout").Default("30s"),
			service.NewDurationField("health_check_interval").Default("10s"),
			service.NewDurationField("connect_timeout").Default("10s")).Optional()).
		Field(service.NewObjectField("observer",
			service.NewIntField("buffer_size").Default(1000),
			service.NewDurationField("observe_timeout").Default("5m"),
			service.NewDurationField("resubscribe_delay").Default("5s")).Optional()).
		Field(service.NewObjectField("retry_policy",
			service.NewIntField("max_retries").Default(3),
			service.NewDurationField("initial_interval").Default("500ms"),
			service.NewDurationField("max_interval").Default("10s"),
			service.NewFloatField("multiplier").Default(1.5),
			service.NewBoolField("jitter").Default(true)).Optional()).
		Field(service.NewObjectField("circuit_breaker",
			service.NewBoolField("enabled").Default(true),
			service.NewIntField("failure_threshold").Default(5),
			service.NewIntField("success_threshold").Default(3),
			service.NewDurationField("timeout").Default("30s"),
			service.NewIntField("half_open_max_calls").Default(2)).Optional()).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(false)).Optional())
			
	pConf, err := configSpec.ParseYAML(confYAML, nil)
	require.NoError(t, err, "Failed to parse CoAP input config YAML")

	input, err := newCoAPInput(pConf, service.MockResources())
	require.NoError(t, err, "Failed to create CoAP input")

	// Note: For input plugins, Connect is typically called by Benthos framework
	// when starting the pipeline. For these tests, we might call it manually
	// or rely on Read calling it if it's designed that way.
	// For observe, Connect should establish the observation.
	err = input.Connect(context.Background())
	require.NoError(t, err, "Failed to connect CoAP input")

	cleanup := func() {
		err := input.Close(context.Background())
		assert.NoError(t, err, "Error closing CoAP input")
	}
	return input, cleanup
}

func TestCoapInput_Integration_BasicObserve(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	observePath := "/test/observe"
	initialData := []byte("Initial data for observe")

	// Add an observable resource to the mock server
	mockServer.AddResource(observePath, coapMessage.TextPlain, initialData, true)

	conf := fmt.Sprintf(`
endpoints:
  - "coap://%s"
observe_paths:
  - "%s"
converter:
  default_content_format: "text/plain"
`, mockServer.Addr(), observePath)

	input, inputCleanup := setupCoapInput(t, conf)
	defer inputCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var receivedMessages []*service.Message
	var mu sync.Mutex
	wg := sync.WaitGroup{}

	// Read messages in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, ackFn, err := input.Read(ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded || err.Error() == "context canceled" {
					return
				}
				return
			}
			if msg != nil {
				mu.Lock()
				receivedMessages = append(receivedMessages, msg)
				mu.Unlock()
				err = ackFn(ctx, nil) // Acknowledge the message
				if err != nil {
					t.Errorf("Ack error: %v", err)
				}
			}
		}
	}()

	// Allow time for initial observation to establish and receive initial notification
	time.Sleep(500 * time.Millisecond)

	// Stop reading messages by canceling the context
	cancel()
	wg.Wait() // Wait for the read loop goroutine to finish

	mu.Lock()
	defer mu.Unlock()

	// Due to mock server limitations with ResponseWriter.SetOptionBytes,
	// the go-coap client library cannot properly handle observe notifications.
	// We can only reliably test the initial observe subscription.
	require.GreaterOrEqual(t, len(receivedMessages), 1, "Should receive at least the initial notification")
	
	// Test the initial message
	initialMsgBytes, err := receivedMessages[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, initialData, initialMsgBytes, "Initial notification payload mismatch")

	// Debug: Print all metadata to verify observer manager is setting path correctly
	t.Logf("All metadata on first message:")
	receivedMessages[0].MetaWalk(func(key, value string) error {
		t.Logf("  %s: %s", key, value)
		return nil
	})

	// Verify the coap_uri_path metadata is correctly set by the observer manager
	pathMeta, exists := receivedMessages[0].MetaGet("coap_uri_path")
	assert.True(t, exists, "coap_uri_path metadata should exist")
	assert.Equal(t, observePath, pathMeta, "coap_uri_path metadata mismatch")

	// Verify other metadata fields
	contentType, exists := receivedMessages[0].MetaGet("coap_content_type")
	assert.True(t, exists, "coap_content_type metadata should exist")
	assert.Equal(t, "text/plain", contentType, "Content type should be text/plain")

	code, exists := receivedMessages[0].MetaGet("coap_code")
	assert.True(t, exists, "coap_code metadata should exist")
	assert.Equal(t, "Content", code, "CoAP code should be Content")

	// Verify observer count on server
	assert.Equal(t, 1, mockServer.GetObserverCount(observePath), "Server should have 1 observer before input close")
}

// TODO: Multiple Observe Paths - DONE
// TODO: Content Format Conversion (from server to Benthos message) - DONE
// TODO: Connection Drop and Resubscription - DONE
// TODO: Handling of Non-Observable Resources - DONE
// TODO: Server Restart During Observation
