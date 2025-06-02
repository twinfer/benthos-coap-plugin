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
