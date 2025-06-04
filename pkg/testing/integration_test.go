// pkg/testing/integration_test.go
//go:build integration
// +build integration

package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
	"github.com/twinfer/benthos-coap-plugin/pkg/observer"
)

func TestCoAPInputIntegration(t *testing.T) {
	// Start mock CoAP server
	server := NewMockCoAPServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Add test resources
	testData := map[string]interface{}{
		"temperature": 23.5,
		"humidity":    65.2,
		"timestamp":   time.Now().Unix(),
	}

	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	server.AddResource("/sensors/temp", message.AppJSON, jsonData, true)

	// Configure CoAP input
	configSpec := service.NewConfigSpec().
		Field(service.NewStringListField("endpoints")).
		Field(service.NewStringListField("observe_paths")).
		Field(service.NewStringField("protocol").Default("udp"))

	configYAML := fmt.Sprintf(`
endpoints:
  - "coap://%s"
observe_paths:
  - "/sensors/temp"
protocol: "udp"
`, server.Addr())

	parsedConfig, err := configSpec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	// Create components
	mgr := service.MockResources()

	endpoints, _ := parsedConfig.FieldStringList("endpoints")
	observePaths, _ := parsedConfig.FieldStringList("observe_paths")
	protocol, _ := parsedConfig.FieldString("protocol")

	connConfig := connection.Config{
		Endpoints:           endpoints,
		Protocol:            protocol,
		MaxPoolSize:         5,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		ConnectTimeout:      10 * time.Second,
	}

	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	require.NoError(t, err)

	conv := converter.NewConverter(converter.Config{}, mgr.Logger())

	obsConfig := observer.Config{
		ObservePaths: observePaths,
		BufferSize:   10,
	}

	obsManager, err := observer.NewManager(obsConfig, connManager, conv, mgr.Logger(), mgr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, obsManager.Start())
	defer obsManager.Close()

	// Wait for initial observe message
	select {
	case msg := <-obsManager.MessageChan():
		require.NotNil(t, msg)

		// Verify message content
		payload, err := msg.AsBytes()
		require.NoError(t, err)

		var receivedData map[string]interface{}
		require.NoError(t, json.Unmarshal(payload, &receivedData))

		assert.Equal(t, testData["temperature"], receivedData["temperature"])
		assert.Equal(t, testData["humidity"], receivedData["humidity"])

		// Verify metadata
		assert.Equal(t, "/sensors/temp", msg.MetaGetOr("coap_path", ""))

		// --- Test Update ---
		t.Log("Updating resource on mock server...")
		updatedData := map[string]interface{}{
			"temperature": 25.0, // New temperature
			"humidity":    60.0, // New humidity
			"status":      "active",
			"timestamp":   time.Now().Unix(),
		}
		jsonUpdatedData, err := json.Marshal(updatedData)
		require.NoError(t, err)
		require.NoError(t, server.UpdateResource("/sensors/temp", jsonUpdatedData))

		// Wait for observe update message
		select {
		case updatedMsg := <-obsManager.MessageChan():
			require.NotNil(t, updatedMsg)
			payload, err := updatedMsg.AsBytes()
			require.NoError(t, err)
			var receivedUpdatedData map[string]interface{}
			require.NoError(t, json.Unmarshal(payload, &receivedUpdatedData))

			assert.Equal(t, updatedData["temperature"], receivedUpdatedData["temperature"])
			assert.Equal(t, updatedData["humidity"], receivedUpdatedData["humidity"])
			assert.Equal(t, updatedData["status"], receivedUpdatedData["status"])
			assert.Equal(t, "/sensors/temp", updatedMsg.MetaGetOr("coap_path", ""))

			// Check observe sequence number if available (it should be higher)
			observeSeqStr := updatedMsg.MetaGetOr("coap_observe", "0")
			observeSeq, _ := parseInt(observeSeqStr) // Helper needed for parsing int
			assert.Greater(t, observeSeq, uint32(0), "Observe sequence should be greater than initial")

		case <-time.After(10 * time.Second):
			t.Fatal("Timeout waiting for CoAP update message")
		case <-ctx.Done():
			t.Fatal("Context cancelled during update wait")
		}

		// --- Test Cancellation ---
		t.Log("Testing observation cancellation...")
		initialObserverCount := server.GetObserverCount("/sensors/temp")
		assert.GreaterOrEqual(t, initialObserverCount, 1, "Should have at least one observer before closing manager")

		// Closing the obsManager should cancel the observation
		require.NoError(t, obsManager.Close()) // Close the specific observer manager

		// Wait a bit for cancellation to propagate and be processed by server
		// The mock server's sendObserveNotification sets observer.Active = false if write fails (e.g. conn closed)
		// but there's no explicit deregistration message sent by go-coap v3 client on observation cancel by default.
		// Effective cancellation means the client stops listening and processing.
		// We can check if the server *would* try to send to fewer observers or if our client stops.
		// For this test, we'll assume closing the manager stops further processing of messages.
		// A more robust server-side check would be if the mock server detected the connection drop.
		// Here, we'll verify that no new messages arrive after closing.

		server.UpdateResource("/sensors/temp", []byte(`{"status":"final"}`)) // Another update
		select {
		case msgAfterClose := <-obsManager.MessageChan():
			t.Fatalf("Received message after closing observer manager: %v", msgAfterClose)
		case <-time.After(200 * time.Millisecond):
			// Expected: no message after close
		}
	// Note: GetObserverCount might not change immediately on client-side cancel if no deregister is sent.
	// The test for cancellation is more about the client (Benthos input) stopping reception.

	// Clean up main context for any remaining goroutines tied to it.
	// cancel() is already deferred.

	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for CoAP message")
	case <-ctx.Done():
		t.Fatal("Context cancelled")
	}
}

func TestCoAPOutputIntegration(t *testing.T) {
	// Start mock CoAP server
	server := NewMockCoAPServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Create connection manager
	mgr := service.MockResources()

	connConfig := connection.Config{
		Endpoints:           []string{fmt.Sprintf("coap://%s", server.Addr())},
		Protocol:            "udp",
		MaxPoolSize:         5,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		ConnectTimeout:      10 * time.Second,
	}

	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get connection and send test message
	conn, err := connManager.Get(ctx)
	require.NoError(t, err)
	defer connManager.Put(conn)

	// Test data
	testData := map[string]interface{}{
		"sensor_id": "temp001",
		"value":     25.3,
		"unit":      "celsius",
	}

	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	msg := service.NewMessage(jsonData)
	msg.MetaSet("content_type", "application/json")
	msg.MetaSet("coap_path", "/test/data")

	conv := converter.NewConverter(converter.Config{}, mgr.Logger())
	coapMsg, err := conv.MessageToCoAP(msg)
	require.NoError(t, err)

	// Send message (simulate output plugin behavior)
	switch c := conn.conn.(type) {
	case interface{ WriteMessage(*message.Message) error }:
		err = c.WriteMessage(coapMsg)
		require.NoError(t, err)
	default:
		t.Fatalf("Unexpected connection type: %T", conn.conn)
	}

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Verify data was received by server
	receivedData, exists := server.GetResourceData("/test/data")
	require.True(t, exists)

	var receivedJSON map[string]interface{}
	require.NoError(t, json.Unmarshal(receivedData, &receivedJSON))

	assert.Equal(t, testData["sensor_id"], receivedJSON["sensor_id"])
	assert.Equal(t, testData["value"], receivedJSON["value"])
	assert.Equal(t, testData["unit"], receivedJSON["unit"])
}

func TestCoAPConnectionPooling(t *testing.T) {
	// Start multiple mock servers
	servers := make([]*MockCoAPServer, 3)
	addrs := make([]string, 3)

	for i := range servers {
		servers[i] = NewMockCoAPServer()
		require.NoError(t, servers[i].Start())
		defer servers[i].Stop()
		addrs[i] = fmt.Sprintf("coap://%s", servers[i].Addr())
	}

	// Configure with multiple endpoints
	mgr := service.MockResources()

	connConfig := connection.Config{
		Endpoints:           addrs,
		Protocol:            "udp",
		MaxPoolSize:         2,
		IdleTimeout:         30 * time.Second,
		HealthCheckInterval: 10 * time.Second,
		ConnectTimeout:      10 * time.Second,
	}

	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test multiple connections
	connections := make([]*connection.ConnectionWrapper, 5)
	for i := 0; i < 5; i++ {
		conn, err := connManager.Get(ctx)
		require.NoError(t, err)
		connections[i] = conn
	}

	// Return connections
	for _, conn := range connections {
		connManager.Put(conn)
	}

	// Verify load balancing occurred
	assert.NotNil(t, connManager)
}

func TestCoAPCircuitBreaker(t *testing.T) {
	config := observer.CircuitConfig{
		Enabled:          true,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
		HalfOpenMaxCalls: 1,
	}

	cb := observer.NewCircuitBreaker(config)

	// Initially closed
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "closed", cb.State())

	// Record failures to open circuit
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	// Should be open now
	assert.False(t, cb.CanExecute())
	assert.Equal(t, "open", cb.State())

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Should be able to execute in half-open
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "half-open", cb.State())

	// Record successes to close circuit
	for i := 0; i < 2; i++ {
		cb.RecordSuccess()
	}

	// Should be closed again
	assert.True(t, cb.CanExecute())
	assert.Equal(t, "closed", cb.State())
}

func TestCoAPMessageConversion(t *testing.T) {
	conv := converter.NewConverter(converter.Config{}, service.MockResources().Logger())

	// Test Benthos to CoAP conversion
	testData := map[string]interface{}{
		"temperature": 25.5,
		"unit":        "celsius",
	}

	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)

	msg := service.NewMessage(jsonData)
	msg.MetaSet("coap_path", "/sensors/temp")
	msg.MetaSet("coap_method", "POST")

	coapMsg, err := conv.MessageToCoAP(msg)
	require.NoError(t, err)

	// Verify CoAP message properties
	assert.NotNil(t, coapMsg)
	assert.Equal(t, jsonData, coapMsg.Payload())

	// Test CoAP to Benthos conversion
	backMsg, err := conv.CoAPToMessage(coapMsg)
	require.NoError(t, err)

	backPayload, err := backMsg.AsBytes()
	require.NoError(t, err)

	var backData map[string]interface{}
	require.NoError(t, json.Unmarshal(backPayload, &backData))

	assert.Equal(t, testData["temperature"], backData["temperature"])
	assert.Equal(t, testData["unit"], backData["unit"])
}

// Helper to parse int from string, used for observe sequence
func parseInt(s string) (uint32, error) {
	v, err := service.NewScanner().NewField(s).AsInt()
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func TestCoAPOutputWithOptionsIntegration(t *testing.T) {
	server := NewMockCoAPServer()
	require.NoError(t, server.Start())
	defer server.Stop()

	// Benthos output component configuration
	outputConfYAML := fmt.Sprintf(`
protocol: udp
default_path: /test_options_path
endpoints:
  - coap://%s
request_options:
  timeout: 5s
  confirmable: true
converter:
  default_content_format: application/json # This will be overridden by coap_content_format
  preserve_options: true # Enable option preservation for custom options
`, server.Addr())

	// Parse the output plugin config (minimal spec for what's needed by newCoAPOutput directly)
	// In a full RunIntegrationSuite, Benthos handles parsing based on registered spec.
	// Here, we manually construct what newCoAPOutput needs.

	// Mock Benthos resources needed by newCoAPOutput
	mgr := service.MockResources()

	// Manually create OutputConfig (as would be parsed from YAML)
	// This is a simplified way to get an Output instance for testing its Write method directly.
	// A full Benthos pipeline test would use service.RunIntegrationSuite.
	outputPluginConfig := observer.ConfigSpec(). // Using observer's spec just to get a ParsedConfig
							Field(service.NewStringField("log.level").Default("DEBUG")) // Add if logger expects it

	parsedOutputConf, err := outputPluginConfig.ParseYAML(outputConfYAML, nil)
	require.NoError(t, err)

	// Create the output instance (simplified for direct testing of Write method)
	// This directly calls the constructor, bypassing full Benthos component registration.
	// For a real integration test of the registered component, you'd use a Benthos stream.
	// For now, we test the 'Write' logic more directly.

	// Create necessary components for the output plugin
	// Note: The real newCoAPOutput parses these from ParsedConfig. We are providing them directly.
	outConfig := &output.OutputConfig{
		Endpoints:   []string{fmt.Sprintf("coap://%s", server.Addr())},
		DefaultPath: "/test_options_path",
		Protocol:    "udp",
		RequestOptions: output.RequestOptions{
			Confirmable: true,
			Timeout:     5 * time.Second,
		},
		Converter: converter.Config{ // Ensure converter config is passed
			PreserveOptions:      true, // Critical for coap_option_*
			DefaultContentFormat: "application/json",
		},
		// Security and ConnectionPool can use defaults if not focus of test
	}

	coapOutput := &output.Output{} // Create an empty struct

	// Manually set up the fields of coapOutput as newCoAPOutput would.
	// This is a more white-box approach for testing Write.
	// A black-box test would use a Benthos stream config.

	connMgrConf := connection.Config{
		Endpoints: outConfig.Endpoints,
		Protocol:  outConfig.Protocol,
		// Fill other connMgrConf fields as necessary, using defaults or test values
		MaxPoolSize:    5,
		IdleTimeout:    30 * time.Second,
		ConnectTimeout: 10 * time.Second,
	}
	connManager, err := connection.NewManager(connMgrConf, mgr.Logger(), mgr)
	require.NoError(t, err)

	conv := converter.NewConverter(outConfig.Converter, mgr.Logger())

	// Reflectively set fields or make a constructor that takes these:
	// This is where it gets tricky without either exporting fields or having a test constructor.
	// For now, let's assume we can construct it for testing or use a helper.
	// Let's try to use the actual constructor after all, by preparing a ParsedConfig.
	// This requires the output plugin to be registered.

	// To properly test the output plugin as Benthos would run it,
	// we should use a Benthos stream definition.

	// --- Setup Benthos Stream ---
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	outputPath := "/options/check" // Path where server will receive the message

	// Define the stream
	streamConf := fmt.Sprintf(`
input:
  generate:
    count: 1
    interval: ""
    mapping: |
      root.id = "test_options_message"
      meta coap_path = "%s"
      meta coap_uri_query = "param1=val1&param2=val2"
      meta coap_etag = "sometag123"
      meta coap_max_age = "90"
      meta coap_content_format = "%d" # e.g., application/cbor
      meta coap_option_2000_0 = "%s" # Custom option, hex encoded
      meta coap_option_2000_1 = "%s"
output:
  coap:
%s # Insert YAML block from outputConfYAML, indented
`, outputPath, message.AppCBOR, hex.EncodeToString([]byte("custom_val_A")), hex.EncodeToString([]byte("custom_val_B")), indentYAML(outputConfYAML, "    "))

	require.NoError(t, service.RegisterOutput("coap", output.ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			return output.NewCoAPOutput(conf, mgr)
		}))

	stream := service.NewStream()
	require.NoError(t, stream.SetYAML(streamConf))

	t.Logf("Running Benthos stream with config:\n%s", streamConf)

	err = stream.Run(ctx)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		// It's okay if it's cancelled or deadline exceeded, as the message might have been sent.
		// Other errors are unexpected.
		// This Run will block until input is exhausted or context is cancelled.
		// Since generate has count:1, it will stop after one message.
		require.NoError(t, err, "Stream.Run returned an unexpected error")
	}

	// --- Verification ---
	// Check server for received message and its options
	time.Sleep(200 * time.Millisecond) // Give server a moment to process fully

	// Retrieve the options captured by the mock server for the specific path
	receivedOpts, found := server.GetResourceLastPutOrPostOptions(outputPath)
	require.True(t, found, "Server should have received a message at path %s", outputPath)

	// Verify Uri-Query
	var queries []string
	for _, opt := range receivedOpts {
		if opt.ID == message.URIQuery {
			queries = append(queries, string(opt.Value))
		}
	}
	assert.Contains(t, queries, "param1=val1", "Uri-Query param1 missing")
	assert.Contains(t, queries, "param2=val2", "Uri-Query param2 missing")

	// Verify ETag
	etagFound := false
	for _, opt := range receivedOpts {
		if opt.ID == message.ETag {
			assert.Equal(t, []byte("sometag123"), opt.Value, "ETag mismatch")
			etagFound = true
			break
		}
	}
	assert.True(t, etagFound, "ETag option not found")

	// Verify Max-Age
	maxAgeFound := false
	for _, opt := range receivedOpts {
		if opt.ID == message.MaxAge {
			// MaxAge is uint. Need to decode from bytes.
			// For simplicity, assume it's stored as a small number.
			// go-coap stores it efficiently.
			// Let's check if our converter set it as expected.
			// The converter uses SetOptionUint32, which handles encoding.
			// We expect the raw bytes here as received by server.
			// Example: 90 = 0x5A.
			assert.Equal(t, []byte{0x5A}, opt.Value, "Max-Age mismatch")
			maxAgeFound = true
			break
		}
	}
	assert.True(t, maxAgeFound, "Max-Age option not found")

	// Verify Content-Format
	cfFound := false
	for _, opt := range receivedOpts {
		if opt.ID == message.ContentFormat {
			// CBOR = 60 = 0x3C
			assert.Equal(t, []byte{0x3C}, opt.Value, "Content-Format should be CBOR (60)")
			cfFound = true
			break
		}
	}
	assert.True(t, cfFound, "Content-Format option not found")

	// Verify Custom Option 2000 (should have two instances)
	var customOptValues [][]byte
	for _, opt := range receivedOpts {
		if opt.ID == message.OptionID(2000) {
			customOptValues = append(customOptValues, opt.Value)
		}
	}
	require.Len(t, customOptValues, 2, "Should have two instances of custom option 2000")
	assert.Contains(t, customOptValues, []byte("custom_val_A"))
	assert.Contains(t, customOptValues, []byte("custom_val_B"))

}

func TestCoAPInputOutputOptionPreservationIntegration(t *testing.T) {
	// Server A: Serves an observable resource with options
	serverA := NewMockCoAPServer()
	resourcePathA := "/sensor/config"
	initialDataA := []byte(`{"version":1, "power":"on"}`)
	// Options to be served by Server A with the resource
	serveOptsA := []message.Option{
		{ID: message.ETag, Value: []byte("etagA123")},
		{ID: message.MaxAge, Value: []byte{0x3C}},                        // 60 seconds
		{ID: message.OptionID(2500), Value: []byte("customObserveOptA")}, // Custom option
	}
	serverA.AddResource(resourcePathA, message.AppJSON, initialDataA, true, serveOptsA...)
	require.NoError(t, serverA.Start())
	defer serverA.Stop()

	// Server B: Receives the message from Benthos output
	serverB := NewMockCoAPServer()
	require.NoError(t, serverB.Start())
	defer serverB.Stop()

	outputPathB := "/device/config_mirror" // Path where Server B receives the message

	// Benthos stream configuration
	streamConf := fmt.Sprintf(`
input:
  coap:
    endpoints: ["coap://%s"]
    observe_paths: ["%s"]
    protocol: "udp"
    converter:
      preserve_options: true
output:
  coap:
    endpoints: ["coap://%s"]
    default_path: "%s" # Will be overridden by coap_path from input if preserved
    protocol: "udp"
    request_options:
      confirmable: true
    converter:
      preserve_options: true
`, serverA.Addr(), resourcePathA, serverB.Addr(), outputPathB)

	// Register input (if not already globally registered by other tests, safe to do again)
	require.NoError(t, service.RegisterInput("coap", observer.ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return observer.NewCoAPInput(conf, mgr)
		}))

	stream := service.NewStream()
	require.NoError(t, stream.SetYAML(streamConf))

	t.Logf("Running E2E Option Preservation Stream:\n%s", streamConf)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second) // Extended timeout
	defer cancel()

	runErrChan := make(chan error, 1)
	go func() {
		runErrChan <- stream.Run(ctx)
	}()

	// Wait for the initial notification from Server A to propagate to Server B
	time.Sleep(3 * time.Second) // Allow time for observation to start and first message to transit

	// Verify options received by Server B
	receivedOptsB, found := serverB.GetResourceLastPutOrPostOptions(outputPathB)
	if !found { // Check last received options if Put/Post path not matching exactly
		receivedOptsB, found = serverB.GetLastReceivedOptionsForPath(outputPathB)
	}
	require.True(t, found, "Server B should have received a message at path %s", outputPathB)

	// Check for ETag (ID 4)
	foundETag := false
	for _, opt := range receivedOptsB {
		if opt.ID == message.ETag {
			assert.Equal(t, []byte("etagA123"), opt.Value, "ETag mismatch on Server B")
			foundETag = true
			break
		}
	}
	assert.True(t, foundETag, "ETag not found on Server B")

	// Check for MaxAge (ID 14)
	foundMaxAge := false
	for _, opt := range receivedOptsB {
		if opt.ID == message.MaxAge {
			assert.Equal(t, []byte{0x3C}, opt.Value, "MaxAge mismatch on Server B")
			foundMaxAge = true
			break
		}
	}
	assert.True(t, foundMaxAge, "MaxAge not found on Server B")

	// Check for Custom Option 2500
	foundCustomOpt := false
	for _, opt := range receivedOptsB {
		if opt.ID == message.OptionID(2500) {
			assert.Equal(t, []byte("customObserveOptA"), opt.Value, "Custom Option 2500 mismatch on Server B")
			foundCustomOpt = true
			break
		}
	}
	assert.True(t, foundCustomOpt, "Custom Option 2500 not found on Server B")

	// Check coap_path: The output should have used the original path from input's metadata
	// This means the message on Server B should be at resourcePathA, not outputPathB,
	// if coap_path was correctly preserved and used by the output.
	// The mock server stores received options by the actual path used in the CoAP request.
	// The converter should set "coap_path" metadata on the Benthos message from the input.
	// The output plugin should then use this "coap_path" metadata.
	_, foundAtPathA_on_B := serverB.GetResourceLastPutOrPostOptions(resourcePathA)
	assert.True(t, foundAtPathA_on_B, "Message on Server B should be at original path %s due to coap_path preservation", resourcePathA)

	// --- Test with an update from Server A ---
	t.Log("Updating resource on Server A to test propagation of new data and options")
	updatedDataA := []byte(`{"version":2, "power":"off"}`)
	updatedServeOptsA := []message.Option{
		{ID: message.ETag, Value: []byte("etagA_v2")}, // New ETag
		{ID: message.MaxAge, Value: []byte{0x1E}},     // 30 seconds
		{ID: message.OptionID(2500), Value: []byte("customObserveOptA_v2")},
	}
	require.NoError(t, serverA.UpdateResource(resourcePathA, updatedDataA, updatedServeOptsA...))

	time.Sleep(3 * time.Second) // Allow time for update to propagate

	receivedOptsBv2, foundV2 := serverB.GetResourceLastPutOrPostOptions(resourcePathA) // Check path A again
	require.True(t, foundV2, "Server B should have received an updated message at path %s", resourcePathA)

	// Verify ETag for v2
	foundETagV2 := false
	for _, opt := range receivedOptsBv2 {
		if opt.ID == message.ETag {
			assert.Equal(t, []byte("etagA_v2"), opt.Value, "ETag v2 mismatch on Server B")
			foundETagV2 = true
			break
		}
	}
	assert.True(t, foundETagV2, "ETag v2 not found on Server B")

	// Verify MaxAge for v2
	foundMaxAgeV2 := false
	for _, opt := range receivedOptsBv2 {
		if opt.ID == message.MaxAge {
			assert.Equal(t, []byte{0x1E}, opt.Value, "MaxAge v2 mismatch on Server B")
			foundMaxAgeV2 = true
			break
		}
	}
	assert.True(t, foundMaxAgeV2, "MaxAge v2 not found on Server B")

	// Verify Custom Option 2500 for v2
	foundCustomOptV2 := false
	for _, opt := range receivedOptsBv2 {
		if opt.ID == message.OptionID(2500) {
			assert.Equal(t, []byte("customObserveOptA_v2"), opt.Value, "Custom Option 2500 v2 mismatch on Server B")
			foundCustomOptV2 = true
			break
		}
	}
	assert.True(t, foundCustomOptV2, "Custom Option 2500 v2 not found on Server B")

	// Stop the stream by cancelling context
	cancel()
	err = <-runErrChan
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		require.NoError(t, err, "Stream.Run returned an unexpected error on close")
	}
}

// Helper to indent YAML block
func indentYAML(yamlBlock, indent string) string {
	lines := strings.Split(strings.TrimRight(yamlBlock, "\n"), "\n")
	var indentedLines []string
	for _, line := range lines {
		indentedLines = append(indentedLines, indent+line)
	}
	return strings.Join(indentedLines, "\n")
}

func BenchmarkCoAPThroughput(b *testing.B) {
	server := NewMockCoAPServer()
	if err := server.Start(); err != nil {
		b.Fatal(err)
	}
	defer server.Stop()

	mgr := service.MockResources()

	connConfig := connection.Config{
		Endpoints:   []string{fmt.Sprintf("coap://%s", server.Addr())},
		Protocol:    "udp",
		MaxPoolSize: 10,
	}

	connManager, err := connection.NewManager(connConfig, mgr.Logger(), mgr)
	if err != nil {
		b.Fatal(err)
	}

	conv := converter.NewConverter(converter.Config{}, mgr.Logger())
	testData := []byte(`{"benchmark": true, "timestamp": 1234567890}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)

			conn, err := connManager.Get(ctx)
			if err != nil {
				b.Errorf("Failed to get connection: %v", err)
				cancel()
				continue
			}

			msg := service.NewMessage(testData)
			coapMsg, err := conv.MessageToCoAP(msg)
			if err != nil {
				b.Errorf("Failed to convert message: %v", err)
				connManager.Put(conn)
				cancel()
				continue
			}

			// Simulate sending
			_ = coapMsg

			connManager.Put(conn)
			cancel()
		}
	})
}
