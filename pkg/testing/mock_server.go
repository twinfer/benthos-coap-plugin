// pkg/testing/mock_server.go
package testing

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	"github.com/plgd-dev/go-coap/v3/net/monitor/inactivity"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
)

// MockCoAPServer provides a test CoAP server for integration testing
type MockCoAPServer struct {
	addr                 string
	listener             *net.UDPConn
	server               *udp.Server
	resources            map[string]*MockResource
	observers            map[string][]Observer
	lastReceivedOptions  map[string][]message.Option // Keyed by path
	lastReceivedMessages map[string]*message.Message // Keyed by path, stores the last message
	mu                   sync.RWMutex
	running              bool
	cancel               context.CancelFunc
}

type MockResource struct {
	Path                 string
	ContentType          message.MediaType
	Data                 []byte
	Observable           bool
	ObserveSeq           uint32
	LastModified         time.Time
	ServeWithOptions     []message.Option // Options to serve on GET for this resource
	LastPutOrPostOptions []message.Option // Options from the last PUT or POST request
}

type Observer struct {
	Token    message.Token
	Response mux.ResponseWriter
	Active   bool
}

func NewMockCoAPServer() *MockCoAPServer {
	return &MockCoAPServer{
		resources:            make(map[string]*MockResource),
		observers:            make(map[string][]Observer),
		lastReceivedOptions:  make(map[string][]message.Option),
		lastReceivedMessages: make(map[string]*message.Message),
	}
}

func (m *MockCoAPServer) AddResource(path string, contentType message.MediaType, data []byte, observable bool, opts ...message.Option) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resources[path] = &MockResource{
		Path:             path,
		ContentType:      contentType,
		Data:             data,
		Observable:       observable,
		ObserveSeq:       0,
		LastModified:     time.Now(),
		ServeWithOptions: opts,
	}
}

func (m *MockCoAPServer) UpdateResource(path string, data []byte, newOpts ...message.Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	resource, exists := m.resources[path]
	if !exists {
		return fmt.Errorf("resource not found: %s", path)
	}

	resource.Data = data
	resource.ObserveSeq++
	resource.LastModified = time.Now()
	if len(newOpts) > 0 { // Allow updating options served by the resource too
		resource.ServeWithOptions = newOpts
	}

	// Notify observers
	if observers, hasObservers := m.observers[path]; hasObservers {
		for i := range observers {
			if observers[i].Active {
				// Use a copy of the observer for the goroutine
				obsCopy := observers[i]
				resCopy := *resource // Use a copy of the resource for the goroutine
				go m.sendObserveNotification(&obsCopy, &resCopy)
			}
		}
	}
	return nil
}

func (m *MockCoAPServer) Start() error {
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return fmt.Errorf("failed to create UDP listener: %w", err)
	}

	m.listener = listener
	m.addr = listener.LocalAddr().String()

	router := mux.NewRouter()
	router.Handle("/", mux.HandlerFunc(m.handleRequest))

	// Create server with proper go-coap v3 configuration
	m.server = udp.NewServer(
		udp.WithMux(router),
		udp.WithInactivityMonitor(100*time.Millisecond, inactivity.NewNilMonitor()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go func() {
		if err := m.server.Serve(listener); err != nil && ctx.Err() == nil {
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
	if m.cancel != nil {
		m.cancel()
	}

	if m.server != nil {
		return m.server.Close()
	}
	return nil
}

func (m *MockCoAPServer) Addr() string {
	return m.addr
}

func (m *MockCoAPServer) handleRequest(w mux.ResponseWriter, r *mux.Message) {
	// Extract path from Uri-Path options
	path, err := r.Options.Path()
	if err != nil || path == "" {
		path = "/"
	}

	switch r.Code() {
	case codes.GET:
		m.handleGet(w, r, path)
	case codes.POST:
		m.captureRequestDetails(path, r.Message)
		m.handlePost(w, r, path)
	case codes.PUT:
		m.captureRequestDetails(path, r.Message)
		m.handlePut(w, r, path)
	case codes.DELETE:
		m.captureRequestDetails(path, r.Message)
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
	if observe, err := r.Options.GetUint32(message.Observe); err == nil && observe == 0 {
		if resource.Observable {
			m.addObserver(path, r.Token(), w)
			m.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)
			w.SetOptionUint32(message.Observe, resource.ObserveSeq) // Observe option is mandatory for observe responses
		} else {
			w.SetResponse(codes.NotAcceptable, message.TextPlain, nil)
		}
	} else {
		m.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)
	}
}

func (m *MockCoAPServer) setResponseWithOptions(w mux.ResponseWriter, code codes.Code, ct message.MediaType, payload []byte, opts []message.Option) {
	w.SetResponse(code, ct, payload)
	for _, opt := range opts {
		w.SetOptionBytes(opt.ID, opt.Value)
	}
}

func (m *MockCoAPServer) captureRequestDetails(path string, req *message.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deep copy options to avoid issues with buffer reuse by the CoAP library
	optsCopy := make([]message.Option, 0, len(req.Options))
	for _, opt := range req.Options {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}
	m.lastReceivedOptions[path] = optsCopy

	// Store a copy of the message
	msgCopy := &message.Message{
		Code:    req.Code,
		Token:   req.Token,
		Type:    req.Type,
		Options: optsCopy,
		Payload: append([]byte(nil), req.Payload...),
	}
	m.lastReceivedMessages[path] = msgCopy
}

func (m *MockCoAPServer) handlePost(w mux.ResponseWriter, r *mux.Message, path string) {
	m.mu.Lock()
	resource, exists := m.resources[path]
	if !exists { // If resource doesn't exist, create it (typical POST behavior)
		resource = &MockResource{Path: path, Observable: false} // Default to not observable for POST-created
		m.resources[path] = resource
	}
	resource.LastPutOrPostOptions = r.Options // Store options from this request
	m.mu.Unlock()

	data := r.Payload
	contentType := message.AppOctets
	if cf, err := r.Options.GetUint32(message.ContentFormat); err == nil {
		contentType = message.MediaType(cf)
	}

	// Update resource with new data and content type
	resource.Data = data
	resource.ContentType = contentType
	resource.LastModified = time.Now()

	// If it was observable, and content changed, we might want to notify observers
	if resource.Observable {
		m.UpdateResource(path, data) // This will also handle notifications
	}

	w.SetResponse(codes.Created, message.TextPlain, nil)
}

func (m *MockCoAPServer) handlePut(w mux.ResponseWriter, r *mux.Message, path string) {
	m.mu.Lock()
	resource, exists := m.resources[path]
	if !exists {
		// PUT usually updates or creates if not present at a specific known URI
		resource = &MockResource{Path: path, Observable: false}
		m.resources[path] = resource
	}
	resource.LastPutOrPostOptions = r.Options
	m.mu.Unlock()

	data := r.Payload
	contentType := message.AppOctets
	if cf, err := r.Options.GetUint32(message.ContentFormat); err == nil {
		contentType = message.MediaType(cf)
	}

	// Update resource data and content type
	resource.Data = data
	resource.ContentType = contentType

	// Call UpdateResource to also handle observer notifications if any
	if err := m.UpdateResource(path, data); err != nil {
		// This case should ideally not happen if we just created the resource above
		w.SetResponse(codes.InternalServerError, message.TextPlain, []byte("failed to update after ensuring resource exists"))
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

	// Set all options defined for the resource, then override with Observe
	m.setResponseWithOptions(observer.Response, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)
	observer.Response.SetOptionUint32(message.Observe, resource.ObserveSeq) // Ensure Observe option is set for notification
}

// Test utilities
func (m *MockCoAPServer) GetObserverCount(path string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	if observers, exists := m.observers[path]; exists {
		for _, obs := range observers {
			if obs.Active { // Only count active observers
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

func (m *MockCoAPServer) GetLastReceivedOptionsForPath(path string) ([]message.Option, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	opts, exists := m.lastReceivedOptions[path]
	if !exists {
		return nil, false
	}
	// Return a copy to prevent modification
	optsCopy := make([]message.Option, len(opts))
	copy(optsCopy, opts)
	return optsCopy, true
}

func (m *MockCoAPServer) GetResourceLastPutOrPostOptions(path string) ([]message.Option, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	resource, exists := m.resources[path]
	if !exists || resource.LastPutOrPostOptions == nil {
		return nil, false
	}
	// Return a copy
	optsCopy := make([]message.Option, len(resource.LastPutOrPostOptions))
	copy(optsCopy, resource.LastPutOrPostOptions)
	return optsCopy, true
}

// MockCoAPClient provides a simple client for testing
type MockCoAPClient struct {
	serverAddr string
	client     *client.Conn
}

func NewMockCoAPClient(serverAddr string) *MockCoAPClient {
	return &MockCoAPClient{
		serverAddr: serverAddr,
	}
}

func (c *MockCoAPClient) Connect(ctx context.Context) error {
	conn, err := udp.Dial(c.serverAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	c.client = conn
	return nil
}

func (c *MockCoAPClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *MockCoAPClient) Get(ctx context.Context, path string) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	msg := &message.Message{
		Code:  codes.GET,
		Token: message.Token("test-get"),
		Type:  message.Confirmable,
	}

	// Set path option
	buf := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetPath(buf, path)

	resp, err := c.client.Do(msg)
	if err != nil {
		return nil, fmt.Errorf("GET request failed: %w", err)
	}

	return resp, nil
}

func (c *MockCoAPClient) Post(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	msg := &message.Message{
		Code:    codes.POST,
		Token:   message.Token("test-post"),
		Type:    message.Confirmable,
		Payload: payload,
	}

	// Set path and content format options
	buf := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetPath(buf, path)
	buf2 := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetContentFormat(buf2, contentType)

	resp, err := c.client.Do(msg)
	if err != nil {
		return nil, fmt.Errorf("POST request failed: %w", err)
	}

	return resp, nil
}

func (c *MockCoAPClient) Put(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	msg := &message.Message{
		Code:    codes.PUT,
		Token:   message.Token("test-put"),
		Type:    message.Confirmable,
		Payload: payload,
	}

	// Set path and content format options
	buf := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetPath(buf, path)
	buf2 := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetContentFormat(buf2, contentType)

	resp, err := c.client.Do(msg)
	if err != nil {
		return nil, fmt.Errorf("PUT request failed: %w", err)
	}

	return resp, nil
}

func (c *MockCoAPClient) Delete(ctx context.Context, path string) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	msg := &message.Message{
		Code:  codes.DELETE,
		Token: message.Token("test-delete"),
		Type:  message.Confirmable,
	}

	// Set path option
	buf := make([]byte, 256)
	msg.Options, _, _ = msg.Options.SetPath(buf, path)

	resp, err := c.client.Do(msg)
	if err != nil {
		return nil, fmt.Errorf("DELETE request failed: %w", err)
	}

	return resp, nil
}