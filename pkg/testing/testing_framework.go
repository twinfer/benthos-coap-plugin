// pkg/testing/mock_server.go
package testing

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	"github.com/plgd-dev/go-coap/v3/udp"
)

// MockCoAPServer provides a test CoAP server for integration testing
type MockCoAPServer struct {
	server    *udp.Server
	addr      string
	resources map[string]*MockResource
	observers map[string][]Observer
	mu        sync.RWMutex
	running   bool
}

type MockResource struct {
	Path         string
	ContentType  message.MediaType
	Data         []byte
	Observable   bool
	ObserveSeq   uint32
	LastModified time.Time
}

type Observer struct {
	Token    message.Token
	Response mux.ResponseWriter
	Active   bool
}

func NewMockCoAPServer() *MockCoAPServer {
	return &MockCoAPServer{
		resources: make(map[string]*MockResource),
		observers: make(map[string][]Observer),
	}
}

func (m *MockCoAPServer) AddResource(path string, contentType message.MediaType, data []byte, observable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resources[path] = &MockResource{
		Path:         path,
		ContentType:  contentType,
		Data:         data,
		Observable:   observable,
		ObserveSeq:   0,
		LastModified: time.Now(),
	}
}

func (m *MockCoAPServer) UpdateResource(path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	resource, exists := m.resources[path]
	if !exists {
		return fmt.Errorf("resource not found: %s", path)
	}

	resource.Data = data
	resource.ObserveSeq++
	resource.LastModified = time.Now()

	// Notify observers
	if observers, hasObservers := m.observers[path]; hasObservers {
		for i := range observers {
			if observers[i].Active {
				go m.sendObserveNotification(&observers[i], resource)
			}
		}
	}

	return nil
}

func (m *MockCoAPServer) Start() error {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return fmt.Errorf("failed to create UDP listener: %w", err)
	}

	m.addr = listener.LocalAddr().String()

	router := mux.NewRouter()
	router.Handle("/", mux.HandlerFunc(m.handleRequest))

	m.server = udp.NewServer(udp.WithMux(router))

	go func() {
		if err := m.server.Serve(listener); err != nil {
			fmt.Printf("CoAP server error: %v\n", err)
		}
	}()

	m.running = true
	return nil
}

func (m *MockCoAPServer) Stop() error {
	if !m.running {
		return nil
	}

	m.running = false
	return m.server.Close()
}

func (m *MockCoAPServer) Addr() string {
	return m.addr
}

func (m *MockCoAPServer) handleRequest(w mux.ResponseWriter, r *mux.Message) {
	path := "/" + r.Route()

	switch r.Code() {
	case codes.GET:
		m.handleGet(w, r, path)
	case codes.POST:
		m.handlePost(w, r, path)
	case codes.PUT:
		m.handlePut(w, r, path)
	case codes.DELETE:
		m.handleDelete(w, r, path)
	default:
		w.SetResponse(codes.MethodNotAllowed, message.TextPlain, nil)
	}
}

func (m *MockCoAPServer) handleGet(w mux.ResponseWriter, r *mux.Message, path string) {
	m.mu.RLock()
	resource, exists := m.resources[path]
	m.mu.RUnlock()

	if !exists {
		w.SetResponse(codes.NotFound, message.TextPlain, nil)
		return
	}

	// Check for observe option
	if observe, err := r.Options().GetUint32(message.Observe); err == nil && observe == 0 {
		if resource.Observable {
			m.addObserver(path, r.Token(), w)
			w.SetResponse(codes.Content, resource.ContentType, resource.Data)
			w.SetOptionUint32(message.Observe, resource.ObserveSeq)
		} else {
			w.SetResponse(codes.NotAcceptable, message.TextPlain, nil)
		}
	} else {
		w.SetResponse(codes.Content, resource.ContentType, resource.Data)
	}
}

func (m *MockCoAPServer) handlePost(w mux.ResponseWriter, r *mux.Message, path string) {
	// Create new resource
	data := r.Payload()
	contentType := message.AppOctets

	if cf, err := r.Options().GetUint32(message.ContentFormat); err == nil {
		contentType = message.MediaType(cf)
	}

	m.AddResource(path, contentType, data, false)
	w.SetResponse(codes.Created, message.TextPlain, nil)
}

func (m *MockCoAPServer) handlePut(w mux.ResponseWriter, r *mux.Message, path string) {
	// Update existing resource
	data := r.Payload()

	if err := m.UpdateResource(path, data); err != nil {
		w.SetResponse(codes.NotFound, message.TextPlain, nil)
		return
	}

	w.SetResponse(codes.Changed, message.TextPlain, nil)
}

func (m *MockCoAPServer) handleDelete(w mux.ResponseWriter, r *mux.Message, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.resources[path]; !exists {
		w.SetResponse(codes.NotFound, message.TextPlain, nil)
		return
	}

	delete(m.resources, path)
	delete(m.observers, path)
	w.SetResponse(codes.Deleted, message.TextPlain, nil)
}

func (m *MockCoAPServer) addObserver(path string, token message.Token, w mux.ResponseWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	observer := Observer{
		Token:    token,
		Response: w,
		Active:   true,
	}

	m.observers[path] = append(m.observers[path], observer)
}

func (m *MockCoAPServer) sendObserveNotification(observer *Observer, resource *MockResource) {
	if !observer.Active {
		return
	}

	err := observer.Response.SetResponse(codes.Content, resource.ContentType, resource.Data)
	if err != nil {
		observer.Active = false
		return
	}

	observer.Response.SetOptionUint32(message.Observe, resource.ObserveSeq)
}

// Test utilities
func (m *MockCoAPServer) GetObserverCount(path string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	if observers, exists := m.observers[path]; exists {
		for _, obs := range observers {
			if obs.Active {
				count++
			}
		}
	}
	return count
}

func (m *MockCoAPServer) GetResourceData(path string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if resource, exists := m.resources[path]; exists {
		return resource.Data, true
	}
	return nil, false
}

// // pkg/testing/integration_test.go
// package testing

// import (
// 	"context"
// 	"encoding/json"
// 	"testing"
// 	"time"

// 	"github.com/redpanda-data/benthos/v4/public/service"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"

// 	"github.com/your-org/benthos-coap/pkg/input"
// )

// func TestCoAPInputIntegration(t *testing.T) {
// 	// Start mock CoAP server
// 	server := NewMockCoAPServer()
// 	require.NoError(t, server.Start())
// 	defer server.Stop()

// 	// Add test resources
// 	testData := map[string]interface{}{
// 		"temperature": 23.5,
// 		"humidity":    65.2,
// 		"timestamp":   time.Now().Unix(),
// 	}

// 	jsonData, err := json.Marshal(testData)
// 	require.NoError(t, err)

// 	server.AddResource("/sensors/temp", message.AppJSON, jsonData, true)

// 	// Configure CoAP input
// 	configYAML := fmt.Sprintf(`
// endpoints:
//   - "coap://%s"
// observe_paths:
//   - "/sensors/temp"
// protocol: "udp"
// connection_pool:
//   max_size: 1
// observer:
//   buffer_size: 10
// `, server.Addr())

// 	config, err := service.NewConfigSpec().ParseYAML(configYAML, nil)
// 	require.NoError(t, err)

// 	// Create and connect input
// 	mgr := service.MockResources()
// 	input, err := newCoAPInput(config, mgr)
// 	require.NoError(t, err)

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	require.NoError(t, input.Connect(ctx))
// 	defer input.Close(ctx)

// 	// Wait for initial observe message
// 	msg, ackFn, err := input.Read(ctx)
// 	require.NoError(t, err)
// 	require.NotNil(t, msg)

// 	// Verify message content
// 	payload, err := msg.AsBytes()
// 	require.NoError(t, err)

// 	var receivedData map[string]interface{}
// 	require.NoError(t, json.Unmarshal(payload, &receivedData))

// 	assert.Equal(t, testData["temperature"], receivedData["temperature"])
// 	assert.Equal(t, testData["humidity"], receivedData["humidity"])

// 	// Verify metadata
// 	assert.Equal(t, "/sensors/temp", msg.MetaGetOr("coap_path", ""))
// 	assert.Equal(t, "application/json", msg.MetaGetOr("coap_content_type", ""))

// 	// Acknowledge message
// 	require.NoError(t, ackFn(ctx, nil))

// 	// Test observe notification
// 	updatedData := map[string]interface{}{
// 		"temperature": 24.1,
// 		"humidity":    66.8,
// 		"timestamp":   time.Now().Unix(),
// 	}

// 	updatedJSON, err := json.Marshal(updatedData)
// 	require.NoError(t, err)

// 	require.NoError(t, server.UpdateResource("/sensors/temp", updatedJSON))

// 	// Read updated message
// 	msg, ackFn, err = input.Read(ctx)
// 	require.NoError(t, err)

// 	payload, err = msg.AsBytes()
// 	require.NoError(t, err)

// 	var updatedReceivedData map[string]interface{}
// 	require.NoError(t, json.Unmarshal(payload, &updatedReceivedData))

// 	assert.Equal(t, updatedData["temperature"], updatedReceivedData["temperature"])
// 	assert.Equal(t, updatedData["humidity"], updatedReceivedData["humidity"])

// 	require.NoError(t, ackFn(ctx, nil))
// }

// func TestCoAPOutputIntegration(t *testing.T) {
// 	// Start mock CoAP server
// 	server := NewMockCoAPServer()
// 	require.NoError(t, server.Start())
// 	defer server.Stop()

// 	// Configure CoAP output
// 	configYAML := fmt.Sprintf(`
// endpoints:
//   - "coap://%s"
// default_path: "/test/data"
// protocol: "udp"
// request_options:
//   confirmable: true
//   timeout: "5s"
// `, server.Addr())

// 	config, err := service.NewConfigSpec().ParseYAML(configYAML, nil)
// 	require.NoError(t, err)

// 	// Create and connect output
// 	mgr := service.MockResources()
// 	output, err := newCoAPOutput(config, mgr)
// 	require.NoError(t, err)

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	require.NoError(t, output.Connect(ctx))
// 	defer output.Close(ctx)

// 	// Send test message
// 	testData := map[string]interface{}{
// 		"sensor_id": "temp001",
// 		"value":     25.3,
// 		"unit":      "celsius",
// 	}

// 	jsonData, err := json.Marshal(testData)
// 	require.NoError(t, err)

// 	msg := service.NewMessage(jsonData)
// 	msg.MetaSet("content_type", "application/json")

// 	require.NoError(t, output.Write(ctx, msg))

// 	// Verify data was received by server
// 	receivedData, exists := server.GetResourceData("/test/data")
// 	require.True(t, exists)

// 	var receivedJSON map[string]interface{}
// 	require.NoError(t, json.Unmarshal(receivedData, &receivedJSON))

// 	assert.Equal(t, testData["sensor_id"], receivedJSON["sensor_id"])
// 	assert.Equal(t, testData["value"], receivedJSON["value"])
// 	assert.Equal(t, testData["unit"], receivedJSON["unit"])
// }

// func TestCoAPConnectionPooling(t *testing.T) {
// 	// Start multiple mock servers
// 	servers := make([]*MockCoAPServer, 3)
// 	for i := range servers {
// 		servers[i] = NewMockCoAPServer()
// 		require.NoError(t, servers[i].Start())
// 		defer servers[i].Stop()
// 	}

// 	// Configure with multiple endpoints
// 	configYAML := fmt.Sprintf(`
// endpoints:
//   - "coap://%s"
//   - "coap://%s"
//   - "coap://%s"
// default_path: "/load/test"
// protocol: "udp"
// connection_pool:
//   max_size: 2
// request_options:
//   confirmable: false
// `, servers[0].Addr(), servers[1].Addr(), servers[2].Addr())

// 	config, err := service.NewConfigSpec().ParseYAML(configYAML, nil)
// 	require.NoError(t, err)

// 	mgr := service.MockResources()
// 	output, err := newCoAPOutput(config, mgr)
// 	require.NoError(t, err)

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	require.NoError(t, output.Connect(ctx))
// 	defer output.Close(ctx)

// 	// Send multiple messages to test load balancing
// 	for i := 0; i < 10; i++ {
// 		testData := map[string]interface{}{
// 			"message_id": i,
// 			"timestamp":  time.Now().Unix(),
// 		}

// 		jsonData, err := json.Marshal(testData)
// 		require.NoError(t, err)

// 		msg := service.NewMessage(jsonData)
// 		require.NoError(t, output.Write(ctx, msg))
// 	}

// 	// Verify messages were distributed across servers
// 	totalMessages := 0
// 	for _, server := range servers {
// 		if _, exists := server.GetResourceData("/load/test"); exists {
// 			totalMessages++
// 		}
// 	}

// 	// At least one server should have received messages
// 	assert.Greater(t, totalMessages, 0)
// }

// func TestCoAPCircuitBreaker(t *testing.T) {
// 	// This test would verify circuit breaker functionality
// 	// by simulating server failures and recovery
// 	t.Skip("Circuit breaker test requires more complex mock server setup")
// }

// func BenchmarkCoAPThroughput(b *testing.B) {
// 	server := NewMockCoAPServer()
// 	require.NoError(b, server.Start())
// 	defer server.Stop()

// 	configYAML := fmt.Sprintf(`
// endpoints:
//   - "coap://%s"
// default_path: "/bench/data"
// protocol: "udp"
// connection_pool:
//   max_size: 10
// request_options:
//   confirmable: false
// `, server.Addr())

// 	config, err := service.NewConfigSpec().ParseYAML(configYAML, nil)
// 	require.NoError(b, err)

// 	mgr := service.MockResources()
// 	output, err := newCoAPOutput(config, mgr)
// 	require.NoError(b, err)

// 	ctx := context.Background()
// 	require.NoError(b, output.Connect(ctx))
// 	defer output.Close(ctx)

// 	testData := []byte(`{"benchmark": true, "timestamp": 1234567890}`)

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			msg := service.NewMessage(testData)
// 			if err := output.Write(ctx, msg); err != nil {
// 				b.Errorf("Write failed: %v", err)
// 			}
// 		}
// 	})
// }
