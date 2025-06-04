//go:build integration

package input

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create and connect CoAP server input, returns the input and a cleanup function.
func setupCoAPServerInput(t *testing.T, confYAML string) (*ServerInput, func()) {
	t.Helper()

	// Parse the config using the server input spec
	pConf, err := coapServerInputConfigSpec.ParseYAML(confYAML, nil)
	require.NoError(t, err, "Failed to parse CoAP server input config YAML")

	input, err := newCoAPServerInput(pConf, service.MockResources())
	require.NoError(t, err, "Failed to create CoAP server input")

	serverInput := input.(*ServerInput)

	// Start the server
	err = serverInput.Connect(context.Background())
	require.NoError(t, err, "Failed to connect CoAP server input")

	cleanup := func() {
		err := serverInput.Close(context.Background())
		assert.NoError(t, err, "Error closing CoAP server input")
	}
	return serverInput, cleanup
}

// Helper to create a CoAP client for testing
func createTestClient(t *testing.T, serverAddr string) (*client.Conn, func()) {
	t.Helper()
	
	conn, err := udp.Dial(serverAddr, options.WithContext(context.Background()))
	require.NoError(t, err, "Failed to create CoAP client")
	
	cleanup := func() {
		conn.Close()
	}
	return conn, cleanup
}

func TestCoAPServerInput_Integration_BasicGET(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/test/path", "/api/data"]
allowed_methods: ["GET", "POST"]
buffer_size: 100
timeout: "5s"
response:
  default_content_format: "text/plain"
  default_code: 69
  default_payload: "OK"
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	// Get the actual address the server is listening on
	serverAddr := serverInput.udpConn.LocalAddr().String()
	t.Logf("Server listening on: %s", serverAddr)

	// Create a client
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

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
			msg, ackFn, err := serverInput.Read(ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return
				}
				t.Logf("Read error: %v", err)
				return
			}
			if msg != nil {
				mu.Lock()
				receivedMessages = append(receivedMessages, msg)
				mu.Unlock()
				err = ackFn(ctx, nil)
				if err != nil {
					t.Errorf("Ack error: %v", err)
				}
			}
		}
	}()

	// Send a GET request
	resp, err := client.Get(ctx, "/test/path")
	require.NoError(t, err, "Failed to send GET request")
	require.NotNil(t, resp)
	assert.Equal(t, codes.Content, resp.Code(), "Response code should be Content (2.05)")
	
	// Read response body
	respPayload, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, []byte("OK"), respPayload, "Response payload should match default")

	// Allow time for message processing
	time.Sleep(100 * time.Millisecond)

	// Cancel context to stop reading
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, receivedMessages, 1, "Should have received 1 message")

	// Check message content
	msg := receivedMessages[0]
	
	// Check metadata
	method, exists := msg.MetaGet("coap_server_method")
	assert.True(t, exists, "coap_server_method should exist")
	assert.Equal(t, "GET", method)

	path, exists := msg.MetaGet("coap_server_path")
	assert.True(t, exists, "coap_server_path should exist")
	assert.Equal(t, "/test/path", path)

	protocol, exists := msg.MetaGet("coap_server_protocol")
	assert.True(t, exists, "coap_server_protocol should exist")
	assert.Equal(t, "udp", protocol)

	remoteAddr, exists := msg.MetaGet("coap_server_remote_addr")
	assert.True(t, exists, "coap_server_remote_addr should exist")
	assert.NotEmpty(t, remoteAddr)

	timestamp, exists := msg.MetaGet("coap_server_timestamp")
	assert.True(t, exists, "coap_server_timestamp should exist")
	assert.NotEmpty(t, timestamp)
}

func TestCoAPServerInput_Integration_POST_WithPayload(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/api/data"]
allowed_methods: ["POST"]
buffer_size: 100
converter:
  default_content_format: "application/json"
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var receivedMessages []*service.Message
	var mu sync.Mutex
	wg := sync.WaitGroup{}

	// Read messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, ackFn, err := serverInput.Read(ctx)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					return
				}
				return
			}
			if msg != nil {
				mu.Lock()
				receivedMessages = append(receivedMessages, msg)
				mu.Unlock()
				ackFn(ctx, nil)
			}
		}
	}()

	// Send a POST request with JSON payload
	payload := []byte(`{"test": "data", "value": 123}`)
	resp, err := client.Post(ctx, "/api/data", message.AppJSON, bytes.NewReader(payload))
	require.NoError(t, err, "Failed to send POST request")
	require.NotNil(t, resp)
	assert.Equal(t, codes.Content, resp.Code())

	// Allow time for message processing
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, receivedMessages, 1, "Should have received 1 message")

	// Check message payload
	msgPayload, err := receivedMessages[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, payload, msgPayload, "Message payload should match POST data")

	// Check CoAP-specific metadata
	contentFormat, exists := receivedMessages[0].MetaGet("coap_content_format")
	assert.True(t, exists, "coap_content_format should exist")
	assert.Equal(t, "50", contentFormat, "Content format should be 50 (AppJSON)")

	contentType, exists := receivedMessages[0].MetaGet("coap_content_type")
	assert.True(t, exists, "coap_content_type should exist")
	assert.Equal(t, "application/json", contentType)

	method, _ := receivedMessages[0].MetaGet("coap_server_method")
	assert.Equal(t, "POST", method)

	path, _ := receivedMessages[0].MetaGet("coap_server_path")
	assert.Equal(t, "/api/data", path)
}

func TestCoAPServerInput_Integration_PathFiltering(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/allowed", "/api/*"]
buffer_size: 100
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	tests := []struct {
		name           string
		path           string
		expectedCode   codes.Code
		shouldReceive  bool
	}{
		{
			name:          "allowed exact path",
			path:          "/allowed",
			expectedCode:  codes.Content,
			shouldReceive: true,
		},
		{
			name:          "disallowed path",
			path:          "/forbidden",
			expectedCode:  codes.NotFound,
			shouldReceive: false,
		},
		{
			name:          "allowed wildcard path",
			path:          "/api/users/123",
			expectedCode:  codes.Content,
			shouldReceive: true,
		},
		{
			name:          "root path when not allowed",
			path:          "/",
			expectedCode:  codes.NotFound,
			shouldReceive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var receivedMessages []*service.Message
			var mu sync.Mutex
			wg := sync.WaitGroup{}

			// Read messages
			wg.Add(1)
			go func() {
				defer wg.Done()
				msgCtx, msgCancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer msgCancel()
				
				msg, ackFn, err := serverInput.Read(msgCtx)
				if err == nil && msg != nil {
					mu.Lock()
					receivedMessages = append(receivedMessages, msg)
					mu.Unlock()
					ackFn(ctx, nil)
				}
			}()

			// Send request
			resp, err := client.Get(ctx, tc.path)
			require.NoError(t, err, "Failed to send GET request to %s", tc.path)
			require.NotNil(t, resp)
			assert.Equal(t, tc.expectedCode, resp.Code(), "Response code mismatch for path %s", tc.path)

			// Wait for goroutine
			wg.Wait()

			mu.Lock()
			if tc.shouldReceive {
				assert.Len(t, receivedMessages, 1, "Should have received message for path %s", tc.path)
				if len(receivedMessages) > 0 {
					path, _ := receivedMessages[0].MetaGet("coap_server_path")
					assert.Equal(t, tc.path, path)
				}
			} else {
				assert.Len(t, receivedMessages, 0, "Should not have received message for path %s", tc.path)
			}
			mu.Unlock()
		})
	}
}

func TestCoAPServerInput_Integration_MethodFiltering(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_methods: ["GET", "POST"]
allowed_paths: ["/test"]
buffer_size: 100
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	ctx := context.Background()

	// Test allowed GET
	resp, err := client.Get(ctx, "/test")
	require.NoError(t, err)
	assert.Equal(t, codes.Content, resp.Code(), "GET should be allowed")

	// Test allowed POST
	resp, err = client.Post(ctx, "/test", message.TextPlain, bytes.NewReader([]byte("test")))
	require.NoError(t, err)
	assert.Equal(t, codes.Content, resp.Code(), "POST should be allowed")

	// Test disallowed PUT
	resp, err = client.Put(ctx, "/test", message.TextPlain, bytes.NewReader([]byte("test")))
	require.NoError(t, err)
	assert.Equal(t, codes.MethodNotAllowed, resp.Code(), "PUT should not be allowed")

	// Test disallowed DELETE
	resp, err = client.Delete(ctx, "/test")
	require.NoError(t, err)
	assert.Equal(t, codes.MethodNotAllowed, resp.Code(), "DELETE should not be allowed")
}

func TestCoAPServerInput_Integration_MultipleClients(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/client/*"]
buffer_size: 100
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()

	// Create multiple clients
	numClients := 5
	clients := make([]*client.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, err := udp.Dial(serverAddr, options.WithContext(context.Background()))
		require.NoError(t, err)
		clients[i] = conn
		defer conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var receivedMessages []*service.Message
	var mu sync.Mutex
	wg := sync.WaitGroup{}

	// Read messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msgCtx, msgCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, ackFn, err := serverInput.Read(msgCtx)
			msgCancel()
			
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if msg != nil {
				mu.Lock()
				receivedMessages = append(receivedMessages, msg)
				mu.Unlock()
				ackFn(ctx, nil)
			}
		}
	}()

	// Send requests from all clients concurrently
	clientWg := sync.WaitGroup{}
	for i, conn := range clients {
		clientWg.Add(1)
		go func(clientNum int, c *client.Conn) {
			defer clientWg.Done()
			path := fmt.Sprintf("/client/%d", clientNum)
			payload := fmt.Sprintf("Message from client %d", clientNum)
			
			resp, err := c.Post(ctx, path, message.TextPlain, bytes.NewReader([]byte(payload)))
			assert.NoError(t, err, "Client %d failed to send POST", clientNum)
			if resp != nil {
				assert.Equal(t, codes.Content, resp.Code(), "Client %d got wrong response code", clientNum)
			}
		}(i, conn)
	}

	// Wait for all clients to finish
	clientWg.Wait()
	
	// Allow time for message processing
	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, receivedMessages, numClients, "Should have received message from each client")

	// Verify we got messages from all clients
	clientPaths := make(map[string]bool)
	for _, msg := range receivedMessages {
		path, _ := msg.MetaGet("coap_server_path")
		clientPaths[path] = true
		
		// Verify message content
		payload, err := msg.AsBytes()
		assert.NoError(t, err)
		assert.Contains(t, string(payload), "Message from client")
	}

	assert.Len(t, clientPaths, numClients, "Should have received from all unique clients")
}

func TestCoAPServerInput_Integration_BufferOverflow(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/overflow/*", "/test/*"]
buffer_size: 2
timeout: "100ms"
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	ctx := context.Background()

	// Send multiple requests quickly to fill the buffer
	for i := 0; i < 5; i++ {
		go func(num int) {
			path := fmt.Sprintf("/overflow/%d", num)
			client.Get(ctx, path)
		}(i)
	}

	// Allow requests to be processed
	time.Sleep(500 * time.Millisecond)

	// The server should handle buffer overflow gracefully
	// Some requests might timeout, but the server should remain operational

	// Verify server is still responsive
	resp, err := client.Get(ctx, "/test/after/overflow")
	require.NoError(t, err, "Server should still be responsive after buffer overflow")
	assert.NotNil(t, resp)
}

func TestCoAPServerInput_Integration_ContentFormats(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/content/*"]
buffer_size: 100
converter:
  preserve_options: true
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	tests := []struct {
		name          string
		contentFormat message.MediaType
		payload       []byte
		expectedType  string
	}{
		{
			name:          "JSON content",
			contentFormat: message.AppJSON,
			payload:       []byte(`{"test": "json"}`),
			expectedType:  "application/json",
		},
		{
			name:          "XML content",
			contentFormat: message.AppXML,
			payload:       []byte(`<test>xml</test>`),
			expectedType:  "application/xml",
		},
		{
			name:          "Plain text",
			contentFormat: message.TextPlain,
			payload:       []byte("plain text"),
			expectedType:  "text/plain",
		},
		{
			name:          "Binary content",
			contentFormat: message.AppOctets,
			payload:       []byte{0x01, 0x02, 0x03, 0x04},
			expectedType:  "application/octet-stream",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var receivedMsg *service.Message
			wg := sync.WaitGroup{}

			// Read message
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg, ackFn, err := serverInput.Read(ctx)
				if err == nil && msg != nil {
					receivedMsg = msg
					ackFn(ctx, nil)
				}
			}()

			// Send request
			resp, err := client.Post(ctx, "/content/test", tc.contentFormat, bytes.NewReader(tc.payload))
			require.NoError(t, err)
			assert.Equal(t, codes.Content, resp.Code())

			// Wait for message
			time.Sleep(100 * time.Millisecond)
			cancel()
			wg.Wait()

			require.NotNil(t, receivedMsg, "Should have received message")

			// Check payload
			msgPayload, err := receivedMsg.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, tc.payload, msgPayload)

			// Check content type metadata
			contentType, exists := receivedMsg.MetaGet("coap_content_type")
			assert.True(t, exists, "coap_content_type should exist")
			assert.Equal(t, tc.expectedType, contentType)
		})
	}
}

func TestCoAPServerInput_Integration_LargePayload(t *testing.T) {
	conf := `
listen_address: "127.0.0.1:0"
protocol: "udp"
allowed_paths: ["/large/*"]
buffer_size: 100
converter:
  max_payload_size: 10000
`

	serverInput, serverCleanup := setupCoAPServerInput(t, conf)
	defer serverCleanup()

	serverAddr := serverInput.udpConn.LocalAddr().String()
	client, clientCleanup := createTestClient(t, serverAddr)
	defer clientCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a large payload (but within CoAP blockwise limits)
	largePayload := make([]byte, 2048)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	var receivedMsg *service.Message
	wg := sync.WaitGroup{}

	// Read message
	wg.Add(1)
	go func() {
		defer wg.Done()
		msg, ackFn, err := serverInput.Read(ctx)
		if err == nil && msg != nil {
			receivedMsg = msg
			ackFn(ctx, nil)
		}
	}()

	// Send large payload
	resp, err := client.Post(ctx, "/large/payload", message.AppOctets, bytes.NewReader(largePayload))
	require.NoError(t, err, "Failed to send large payload")
	assert.Equal(t, codes.Content, resp.Code())

	// Wait for message
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	require.NotNil(t, receivedMsg, "Should have received message with large payload")

	// Verify payload integrity
	msgPayload, err := receivedMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, largePayload, msgPayload, "Large payload should be received intact")
}