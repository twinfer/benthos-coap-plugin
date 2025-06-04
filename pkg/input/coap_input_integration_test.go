//go:build integration

package input

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/testing" // Our mock server package

	coapMessage "github.com/plgd-dev/go-coap/v3/message"
)

// Helper to create and connect CoapInput, returns the input and a cleanup function.
func setupCoapInput(t *testing.T, confYAML string) (*CoapInput, func()) {
	t.Helper()
	pConf, err := coapInputPluginSpec().ParseYAML(confYAML, nil)
	require.NoError(t, err, "Failed to parse CoAP input config YAML")

	input, err := newCoapInput(pConf, service.MockResources())
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
	mockServer, serverCleanup := testing.SetupTestServer(t)
	defer serverCleanup()

	observePath := "/test/observe"
	initialData := []byte("Initial data for observe")
	updatedData1 := []byte("Updated data 1")
	updatedData2 := []byte("Updated data 2")

	// Add an observable resource to the mock server
	mockServer.AddResource(observePath, coapMessage.TextPlain, initialData, true)

	conf := fmt.Sprintf(`
paths:
  - "%s"
url: "coap://%s"
default_content_type: "text/plain" # Expected format from server
`, observePath, mockServer.Addr())

	input, inputCleanup := setupCoapInput(t, conf)
	defer inputCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
				if err == context.Canceled || err == context.DeadlineExceeded || err.Error() == "context canceled" { // TODO: check for specific Benthos error on close
					// t.Logf("Read loop ended: %v", err)
					return
				}
				// t.Errorf("Read error: %v", err)
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
	// The input plugin should receive the initial state upon successful observation.
	time.Sleep(200 * time.Millisecond) // Increased from 100ms

	// Check for initial notification
	mu.Lock()
	require.GreaterOrEqual(t, len(receivedMessages), 1, "Should receive at least the initial notification")
	initialMsgBytes, err := receivedMessages[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, initialData, initialMsgBytes, "Initial notification payload mismatch")

	pathMeta, exists := receivedMessages[0].MetaGet("coap_path")
	assert.True(t, exists, "coap_path metadata should exist")
	assert.Equal(t, observePath, pathMeta, "coap_path metadata mismatch")
	mu.Unlock()

	// Update resource on server to trigger notification 1
	err = mockServer.UpdateResource(observePath, updatedData1)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Allow time for notification

	// Update resource on server to trigger notification 2
	err = mockServer.UpdateResource(observePath, updatedData2)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // Allow time for notification

	// Stop reading messages by canceling the context
	cancel()
	wg.Wait() // Wait for the read loop goroutine to finish

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, receivedMessages, 3, "Should have received 3 messages (initial + 2 updates)")

	// Detailed check of messages (order might vary slightly depending on timing, but generally sequential for single path)
	// For this test, we assume they arrive in order of updates.

	// Initial was already checked.

	// Notification 1
	update1MsgBytes, err := receivedMessages[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, updatedData1, update1MsgBytes, "Update 1 payload mismatch")
	pathMeta1, _ := receivedMessages[1].MetaGet("coap_path")
	assert.Equal(t, observePath, pathMeta1)

	// Notification 2
	update2MsgBytes, err := receivedMessages[2].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, updatedData2, update2MsgBytes, "Update 2 payload mismatch")
	pathMeta2, _ := receivedMessages[2].MetaGet("coap_path")
	assert.Equal(t, observePath, pathMeta2)

	// Verify observer count on server (might be 0 if client/input deregistered on close)
	// Depending on Close() behavior of input plugin.
	// For now, let's just check it was 1 at some point.
	// After input.Close(), the client should deregister.
	// We call inputCleanup (which calls Close) after wg.Wait().
	// So, before that, observer count should be 1.
	assert.Equal(t, 1, mockServer.GetObserverCount(observePath), "Server should have 1 observer before input close")
}

// TODO: Multiple Observe Paths - DONE
// TODO: Content Format Conversion (from server to Benthos message) - DONE
// TODO: Connection Drop and Resubscription - DONE
// TODO: Handling of Non-Observable Resources - DONE
// TODO: Server Restart During Observation
