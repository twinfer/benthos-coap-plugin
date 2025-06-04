// pkg/testing/mock_server.go
package testing

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net" // Add this import
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/plgd-dev/go-coap/v3/udp/server"
)

// MockCoAPServer provides a test CoAP server for integration testing
type MockCoAPServer struct {
	addr                 string
	listener             *coapNet.UDPConn // Use the go-coap specific UDPConn type
	server               *server.Server
	resources            map[string]*MockResource
	observers            map[string][]Observer
	lastReceivedOptions  map[string][]message.Option // Keyed by path
	lastReceivedMessages map[string]*message.Message // Keyed by path, stores the last message
	mu                   sync.RWMutex
	running              bool
	cancel               context.CancelFunc
}

// responseWriterWithOptions is a local interface to allow setting options on the ResponseWriter.
// This is a workaround because mux.ResponseWriter interface in v3.3.6 doesn't expose SetOptionBytes,
// but the concrete implementations passed by the server do.
type responseWriterWithOptions interface {
	mux.ResponseWriter
	SetOptionBytes(id message.OptionID, value []byte) error
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
	// Create the address string for NewListenUDP
	listenAddrStr := (&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}).String()
	listener, err := coapNet.NewListenUDP("udp", listenAddrStr)
	if err != nil {
		return fmt.Errorf("failed to create UDP listener: %w", err)
	}

	m.listener = listener
	m.addr = listener.LocalAddr().String()

	router := mux.NewRouter()
	router.Handle("/", mux.HandlerFunc(m.handleRequest))

	// Create server with proper go-coap v3 configuration
	m.server = server.New( // Use the 'server' alias for github.com/plgd-dev/go-coap/v3/udp/server
		options.WithMux(router), // WithMux is in the 'options' package
	)

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	go func() { // Serve blocks, run in goroutine
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

	return nil
}

func (m *MockCoAPServer) Addr() string {
	return m.addr
}

func (m *MockCoAPServer) handleRequest(w mux.ResponseWriter, r *mux.Message) {
	// Extract path from Uri-Path options
	opts := r.Options()
	path, err := opts.Path()
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
	opts := r.Options()
	if observeVal, err := opts.GetUint32(message.Observe); err == nil && observeVal == 0 { // Observe registration
		if resource.Observable {
			m.addObserver(path, r.Token(), w)
			// Send initial response with Observe option
			m.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)

			// Assert to our local interface that includes SetOptionBytes
			rw, ok := w.(responseWriterWithOptions)
			if !ok {
				fmt.Printf("MockCoAPServer: ResponseWriter does not implement SetOptionBytes for Observe option\n")
				return // Cannot set Observe option
			}

			// Encode Observe sequence number (uint32) to bytes using a buffer
			buf := make([]byte, 4) // Max size for uint32 CoAP encoding (CoAP spec)
			resultBuffer, bytesWritten := message.EncodeUint32(buf, resource.ObserveSeq)
			obsBytes := resultBuffer[:bytesWritten] // Use the byte slice and the count correctly
			if err := w.SetOptionBytes(message.Observe, obsBytes); err != nil {
				fmt.Printf("MockCoAPServer: failed to set Observe option (bytes) on initial GET for path %s: %v\n", path, err)
			} else {
				fmt.Printf("MockCoAPServer: Successfully set Observe option (seq: %d) on initial GET for path %s\n", resource.ObserveSeq, path)
			}
		} else {
			// Resource is not observable
			if err := w.SetResponse(codes.NotAcceptable, message.TextPlain, nil); err != nil {
				fmt.Printf("MockCoAPServer: failed to set NotAcceptable response for non-observable GET on path %s: %v\n", path, err)
			}
		}
	} else { // Regular GET
		m.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)
	}
}

func (m *MockCoAPServer) setResponseWithOptions(w mux.ResponseWriter, code codes.Code, ct message.MediaType, payload []byte, opts []message.Option) {
	// Assert to our local interface that includes SetOptionBytes
	rw, ok := w.(responseWriterWithOptions)
	if !ok {
		// This should not happen with go-coap v3.3.6 server implementations,
		// but handle defensively.
		fmt.Printf("MockCoAPServer: ResponseWriter does not implement SetOptionBytes\n")
		// Still try to set the basic response
		if err := w.SetResponse(code, ct, payload); err != nil {
			fmt.Printf("MockCoAPServer: error setting response (code: %v, path: %s): %v\n", code, w.Conn().Request().URIPath(), err)
		}
		return
	}

	if err := rw.SetResponse(code, ct, payload); err != nil {
		fmt.Printf("MockCoAPServer: error setting response (code: %v, path: %s): %v\n", code, w.Conn().Request().URIPath(), err)
	}
	for _, opt := range opts {
		if err := w.SetOptionBytes(opt.ID, opt.Value); err != nil {
			fmt.Printf("MockCoAPServer: error setting option (ID: %v, path: %s): %v\n", opt.ID, w.Conn().Request().URIPath(), err)
		}
	}
}

func (m *MockCoAPServer) captureRequestDetails(path string, req *pool.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get options from pool.Message
	opts := req.Options()
	optsCopy := make([]message.Option, 0, len(opts))
	for _, opt := range opts {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}
	m.lastReceivedOptions[path] = optsCopy

	// Get payload from pool.Message body
	var payload []byte
	if body := req.Body(); body != nil {
		if data, err := io.ReadAll(body); err == nil {
			payload = data
		}
	}

	// Store a copy of the message
	msgCopy := &message.Message{
		Code:    req.Code(),
		Token:   req.Token(),
		Type:    req.Type(),
		Options: optsCopy,
		Payload: payload,
	}
	m.lastReceivedMessages[path] = msgCopy
}

func (m *MockCoAPServer) handlePost(w mux.ResponseWriter, r *mux.Message, path string) {
	m.mu.Lock()
	// Deep copy options from the request
	reqOpts := r.Options()
	optsCopy := make([]message.Option, 0, len(reqOpts))
	for _, opt := range reqOpts {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}

	resource, exists := m.resources[path]
	if !exists { // If resource doesn't exist, create it (typical POST behavior)
		resource = &MockResource{Path: path, Observable: false} // Default to not observable for POST-created
		m.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy // Store copied options
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
	// Deep copy options from the request
	reqOpts := r.Options()
	optsCopy := make([]message.Option, 0, len(reqOpts))
	for _, opt := range reqOpts {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}

	resource, exists := m.resources[path]
	if !exists {
		// PUT usually updates or creates if not present at a specific known URI
		resource = &MockResource{Path: path, Observable: false}
		m.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy // Store copied options
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
	// The ResponseWriter (w) for an observer is the one from the initial GET request.
	m.setResponseWithOptions(observer.Response, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions)

	// Assert to our local interface that includes SetOptionBytes
	rw, ok := observer.Response.(responseWriterWithOptions)
	if !ok {
		fmt.Printf("MockCoAPServer: Observer ResponseWriter does not implement SetOptionBytes for notification\n")
		return // Cannot set Observe option on notification
	}

	// Encode Observe sequence number (uint32) to bytes using a buffer
	buf := make([]byte, 4) // Max size for uint32 CoAP encoding (CoAP spec)
	resultBuffer, bytesWritten := message.EncodeUint32(buf, resource.ObserveSeq)
	obsBytes := resultBuffer[:bytesWritten] // Use the byte slice and the count correctly
	if err := observer.Response.SetOptionBytes(message.Observe, obsBytes); err != nil {
		fmt.Printf("MockCoAPServer: error setting Observe option (bytes) on notification for path %s: %v\n", resource.Path, err)
	} else {
		fmt.Printf("MockCoAPServer: Successfully set Observe option (seq: %d) on notification for path %s\n", resource.ObserveSeq, resource.Path)
	}
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
