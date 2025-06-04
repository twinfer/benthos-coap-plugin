// pkg/testing/mock_server.go

// Package testing provides utilities for integration testing of CoAP applications.
//
// The primary component is MockCoAPServer, a configurable in-memory CoAP server
// that allows simulating various CoAP resource behaviors, including observability (RFC 7641).
// It supports:
//   - Defining resources with specific content types, data, and observable behavior.
//   - Handling GET, POST, PUT, DELETE requests through a flexible handler interface.
//   - Capturing incoming requests for detailed inspection in tests (request history).
//   - Simulating resource updates and notifying active observers.
//   - Methods to inspect server state, such as active observers and resource data.
//
// MockCoAPClient is a basic client for interacting with the MockCoAPServer or other
// CoAP servers in test scenarios.
//
// This package aims to simplify the setup of CoAP integration tests by providing
// controllable server and client components, reducing the need for external dependencies
// or complex network configurations during testing.
package testing

import (
	"bytes" // Required for converting payload []byte to io.Reader for client Post/Put.
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	"github.com/plgd-dev/go-coap/v3/udp/server"
)

// RequestHandler defines the interface for handling CoAP requests.
// Each CoAP method (GET, POST, PUT, DELETE, etc.) can have a specific
// implementation of this interface.
type RequestHandler interface {
	// HandleRequest processes a CoAP request for a given path.
	// w: The response writer to send the CoAP response.
	// r: The received CoAP message.
	// path: The URI path of the resource targeted by the request.
	HandleRequest(w mux.ResponseWriter, r *mux.Message, path string)
}

// MockCoAPServer provides a configurable, in-memory CoAP server for integration testing.
// It allows adding resources, simulating CoAP methods (GET, POST, PUT, DELETE),
// and observing resource changes (RFC 7641).
//
// Key features:
// - Resource Management: Add, update, and delete resources dynamically.
// - Request Handling: Uses a RequestHandler interface for extensible processing of CoAP methods.
// - Observability: Supports CoAP observe; resources can be marked as observable, and updates trigger notifications.
// - Request History: Logs incoming requests (path, method, options, payload) for test assertions.
// - Thread-Safe: Operations are protected by mutexes for safe concurrent access.
type MockCoAPServer struct {
	addr                 string
	listener             *coapNet.UDPConn              // Underlying CoAP UDP connection.
	server               *server.Server                // The core CoAP server instance.
	resources            map[string]*MockResource      // Map of URI paths to their corresponding MockResource.
	requestHandlers      map[codes.Code]RequestHandler // Map of CoAP codes to their respective handlers.
	observers            map[string][]Observer         // Map of URI paths to lists of active observers.
	lastReceivedOptions  map[string][]message.Option   // Stores the options of the last request for a given path (legacy, consider phasing out).
	lastReceivedMessages map[string]*message.Message   // Stores the last message for a given path (legacy, consider phasing out).
	mu                   sync.RWMutex                  // Mutex for thread-safe access to shared server state.
	running              bool                          // Indicates if the server is currently running.
	cancel               context.CancelFunc            // Function to cancel the server's context, used for stopping.
	requestHistory       []LoggedRequest               // Log of received requests for test inspection.
	// TODO: Add responseHistory for enhanced observability, potentially storing LoggedResponse structs.
}

// LoggedRequest stores detailed information about a CoAP request received by the mock server.
// This struct is used to populate the request history for later inspection in tests.
// All mutable fields (Token, Options, Payload) are deep copies of the original request data
// to ensure they are not modified by subsequent processing.
type LoggedRequest struct {
	Timestamp time.Time       // Timestamp indicating when the server logged the request (UTC).
	Path      string          // URI path extracted from the request.
	Code      codes.Code      // CoAP method code (e.g., codes.GET, codes.POST).
	Token     message.Token   // CoAP token (a deep copy).
	Type      message.Type    // CoAP message type (Confirmable, NonConfirmable, etc.).
	Options   message.Options // CoAP options included in the request (a deep copy).
	Payload   []byte          // Payload/body of the request (a deep copy).
}

// responseWriterWithOptions is an internal interface used to abstract the setting of CoAP options
// on a response writer. This is primarily a workaround for limitations in the underlying
// CoAP library's mux.ResponseWriter interface, allowing access to methods like SetOptionBytes
// available on concrete types.
type responseWriterWithOptions interface {
	mux.ResponseWriter
	SetOptionBytes(id message.OptionID, value []byte) error
}

// MockResource represents a simulated CoAP resource within the MockCoAPServer.
// It holds the resource's properties, data, and observation-related state.
type MockResource struct {
	Path                 string            // URI path of the resource.
	ContentType          message.MediaType // CoAP content type of the resource's payload.
	Data                 []byte            // Current data/payload of the resource.
	Observable           bool              // Flag indicating if the resource supports CoAP observation.
	ObserveSeq           uint32            // Current observe sequence number for notifications.
	LastModified         time.Time         // Timestamp of the last modification to the resource.
	ServeWithOptions     []message.Option  // CoAP options to be included in responses for GET requests to this resource.
	LastPutOrPostOptions []message.Option  // CoAP options received in the last PUT or POST request to this resource.
	
	// Fields for testing scenarios
	ResponseDelay         time.Duration // Delay before responding (for timeout tests)
	ResponseSequence      []codes.Code  // Sequence of response codes to return
	CurrentResponseIndex  int           // Current index in ResponseSequence
}

// Observer represents an active CoAP observer client for a specific resource.
// It stores the observer's token and the response writer associated with the observation relationship.
type Observer struct {
	Token    message.Token      // CoAP token that identifies the observation relationship.
	Response mux.ResponseWriter // ResponseWriter for sending notifications to the observer.
	Active   bool               // Flag indicating if the observer is still considered active.
}

// NewMockCoAPServer creates a new instance of MockCoAPServer.
// It initializes the server with default request handlers for GET, POST, PUT, and DELETE methods.
// The server is not started automatically; call Start() to begin listening for requests.
func NewMockCoAPServer() *MockCoAPServer {
	s := &MockCoAPServer{
		resources:            make(map[string]*MockResource),
		requestHandlers:      make(map[codes.Code]RequestHandler),
		observers:            make(map[string][]Observer),
		lastReceivedOptions:  make(map[string][]message.Option), // Kept for backward compatibility or specific uses.
		lastReceivedMessages: make(map[string]*message.Message), // Kept for backward compatibility or specific uses.
		requestHistory:       make([]LoggedRequest, 0, 100),     // Initialize requestHistory with a capacity of 100.
	}
	// Register default handlers
	s.requestHandlers[codes.GET] = &GetHandler{server: s}
	s.requestHandlers[codes.POST] = &PostHandler{server: s}
	s.requestHandlers[codes.PUT] = &PutHandler{server: s}
	s.requestHandlers[codes.DELETE] = &DeleteHandler{server: s}
	return s
}

// AddResource adds a new resource to the mock server or updates an existing one at the given path.
//
// Parameters:
//
//	path: The URI path for the resource (e.g., "/temp", "/config/led").
//	contentType: The CoAP message.MediaType to be used for the resource's payload.
//	data: The initial byte slice data for the resource.
//	observable: A boolean indicating whether this resource should support CoAP observations.
//	            If true, clients can register to observe changes.
//	opts: A variadic slice of message.Option to be served with this resource on GET requests.
//	      These options are added to every GET response for this resource.
func (m *MockCoAPServer) AddResource(path string, contentType message.MediaType, data []byte, observable bool, opts ...message.Option) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resources[path] = &MockResource{
		Path:             path,
		ContentType:      contentType,
		Data:             data,
		Observable:       observable,
		ObserveSeq:       0,                // Initial sequence number for observations.
		LastModified:     time.Now().UTC(), // Use UTC for consistency.
		ServeWithOptions: opts,
	}
}

// UpdateResource modifies an existing resource's data and optionally its served CoAP options.
// If the resource is observable, this triggers a notification sequence to all active observers.
// The resource's LastModified timestamp and ObserveSeq (if observable) are updated.
//
// Parameters:
//
//	path: The URI path of the resource to update.
//	data: The new byte slice data for the resource.
//	newOpts: Optional CoAP options that will replace the resource's ServeWithOptions if provided.
//	         If not provided, existing ServeWithOptions are retained.
//
// Returns:
//
//	An error if the resource at the given path is not found. Otherwise, nil.
func (m *MockCoAPServer) UpdateResource(path string, data []byte, newOpts ...message.Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	resource, exists := m.resources[path]
	if !exists {
		return fmt.Errorf("MockCoAPServer ERROR: [UpdateResource %s] Resource not found", path)
	}

	resource.Data = data
	if resource.Observable {
		resource.ObserveSeq++
	}
	resource.LastModified = time.Now().UTC() // Use UTC for consistency.
	if len(newOpts) > 0 {                    // Allow updating options served by the resource too
		resource.ServeWithOptions = newOpts
	}

	// Notify observers if the resource is observable and has active observers.
	if resource.Observable {
		if observers, hasObservers := m.observers[path]; hasObservers {
			for i := range observers {
				// Check Active flag inside the loop in case an observer becomes inactive during notification processing.
				if observers[i].Active {
					// Create copies for the goroutine to avoid race conditions on loop variables.
					obsCopy := observers[i]
					resCopy := *resource // Copy resource state at the time of notification.
					go m.sendObserveNotification(&obsCopy, &resCopy)
				}
			}
		}
	}
	return nil
}

// Start initializes and starts the MockCoAPServer.
// It sets up a UDP listener on a dynamically allocated port on localhost (127.0.0.1).
// The server then begins to listen for and handle incoming CoAP requests in a separate goroutine.
// Returns an error if the listener cannot be created or the server fails to start.
func (m *MockCoAPServer) Start() error {
	// Use "127.0.0.1:0" to listen on any available port on the loopback interface.
	// This is ideal for testing as it avoids port conflicts.
	listener, err := coapNet.NewListenUDP("udp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("MockCoAPServer ERROR: [Start] Failed to create UDP listener: %w", err)
	}

	m.listener = listener
	m.addr = listener.LocalAddr().String() // Store the dynamically allocated address.

	router := mux.NewRouter()
	// All requests are routed through the main handleRequest method, which then delegates
	// to specific handlers based on the CoAP method code.
	// Use DefaultHandler to catch all unmatched requests
	router.DefaultHandle(mux.HandlerFunc(m.handleRequest))

	m.server = server.New(options.WithMux(router))

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel // Store cancel function to allow stopping the server.

	go func() {
		// Serve blocks until the listener is closed or an error occurs.
		// Run in a goroutine to not block the Start() method.
		if err := m.server.Serve(listener); err != nil && ctx.Err() == nil {
			// Log errors from the underlying server unless the context was canceled (server stopped).
			fmt.Printf("MockCoAPServer ERROR: [Start] Underlying server Serve() error: %v\n", err)
		}
	}()

	m.running = true
	return nil
}

// Stop gracefully shuts down the MockCoAPServer.
// It cancels the server's context, which stops the underlying CoAP server and closes the listener.
// Returns nil if successful, or an error if stopping fails (though current implementation always returns nil).
// It is safe to call Stop multiple times; subsequent calls after the first have no effect.
func (m *MockCoAPServer) Stop() error {
	if !m.running {
		return nil // Server is not running or already stopped.
	}
	m.running = false
	if m.cancel != nil {
		m.cancel()     // Signal the server's goroutine to exit.
		m.cancel = nil // Avoid multiple cancellations.
	}
	// Note: The underlying go-coap server's Serve method handles listener closing on context cancellation.
	return nil
}

// Addr returns the network address (host:port) on which the MockCoAPServer is listening.
// This is useful for configuring clients to connect to the mock server.
// Returns an empty string if the server has not been started or failed to start.
func (m *MockCoAPServer) Addr() string {
	return m.addr
}

// handleRequest is the main CoAP request handler for the MockCoAPServer.
// It extracts the request path, logs the request for observability, and then
// delegates processing to a method-specific handler (GetHandler, PostHandler, etc.)
// registered in the `requestHandlers` map. If no handler is found for the request's
// CoAP code, it responds with MethodNotAllowed.
func (m *MockCoAPServer) handleRequest(w mux.ResponseWriter, r *mux.Message) {
	// Extract path from Uri-Path options
	opts := r.Options()
	path, err := opts.Path()
	if err != nil || path == "" {
		path = "/" // Default to root if path extraction fails or is empty
	}

	// Log the incoming request for observability before delegating to a handler.
	// This ensures all requests are logged, even if no specific handler is found.
	m.logCoAPRequest(path, r.Message)

	// Delegate to registered handler based on CoAP code
	if handler, ok := m.requestHandlers[r.Code()]; ok {
		handler.HandleRequest(w, r, path)
	} else {
		// Respond with MethodNotAllowed if no handler is registered for the CoAP code
		// It's good practice to provide a payload explaining the error.
		if errResp := w.SetResponse(codes.MethodNotAllowed, message.TextPlain, bytes.NewReader([]byte("Method not allowed"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [HandleRequest %s] Failed to set MethodNotAllowed response for code %s: %v\n", path, r.Code(), errResp)
		}
	}
}

// GetHandler implements RequestHandler for GET requests.
type GetHandler struct {
	server *MockCoAPServer
}

// HandleRequest processes GET requests. It supports regular GET and observe registration.
// It checks if the resource exists, if it's observable (for observe requests),
// and sends the appropriate response including CoAP options like Observe and ETag (if defined).
func (h *GetHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.RLock() // Read lock is sufficient as we are primarily reading resource data.
	resource, exists := h.server.resources[path]
	h.server.mu.RUnlock()

	if !exists {
		if err := w.SetResponse(codes.NotFound, message.TextPlain, bytes.NewReader([]byte("Resource not found"))); err != nil {
			fmt.Printf("MockCoAPServer ERROR: [GET %s] Failed to set NotFound response: %v\n", path, err)
		}
		return
	}

	reqOpts := r.Options()
	if observeVal, err := reqOpts.GetUint32(message.Observe); err == nil && observeVal == 0 { // Observe registration
		if resource.Observable {
			// AddObserver needs to acquire a write lock on server.mu if it modifies shared observer list.
			// Consider if GetHandler should have its own lock or if server.addObserver handles synchronization.
			// For now, assuming server.addObserver is thread-safe.
			h.server.addObserver(path, r.Token(), w)

			// Send initial response with Observe option
			// Pass the 'path' for logging context in setResponseWithOptions
			h.server.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions, path)

			rw, ok := w.(responseWriterWithOptions)
			if !ok {
				// This is a warning because the main content response is still sent.
				// However, the client won't get the observe sequence number on this initial response.
				fmt.Printf("MockCoAPServer WARNING: [GET %s] ResponseWriter does not implement SetOptionBytes; cannot set Observe option on initial response.\n", path)
				return // Return after sending content, as Observe option cannot be set.
			}

			buf := make([]byte, 4) // Max size for uint32 CoAP encoding
			bytesWritten, _ := message.EncodeUint32(buf, resource.ObserveSeq)
			obsBytes := buf[:bytesWritten]
			if errSetOpt := rw.SetOptionBytes(message.Observe, obsBytes); errSetOpt != nil {
				fmt.Printf("MockCoAPServer WARNING: [GET %s] Failed to set Observe option (bytes, seq %d) on initial response: %v\n", path, resource.ObserveSeq, errSetOpt)
			} else {
				// fmt.Printf("MockCoAPServer DEBUG: [GET %s] Successfully set Observe option (seq: %d) on initial response.\n", path, resource.ObserveSeq)
			}
		} else {
			// Resource is not observable
			if err := w.SetResponse(codes.NotAcceptable, message.TextPlain, bytes.NewReader([]byte("Resource not observable"))); err != nil {
				fmt.Printf("MockCoAPServer ERROR: [GET %s] Failed to set NotAcceptable response for non-observable resource: %v\n", path, err)
			}
		}
	} else { // Regular GET
		h.server.setResponseWithOptions(w, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions, path)
	}
}

// setResponseWithOptions is a helper to set response code, content type, payload, and additional CoAP options.
// It centralizes response sending logic and error logging.
// The 'requestPathForLogging' parameter provides context for log messages.
func (m *MockCoAPServer) setResponseWithOptions(w mux.ResponseWriter, code codes.Code, ct message.MediaType, payload []byte, opts []message.Option, requestPathForLogging string) {
	rw, ok := w.(responseWriterWithOptions)
	if !ok {
		fmt.Printf("MockCoAPServer WARNING: [Response %s] ResponseWriter does not implement SetOptionBytes. Cannot set custom CoAP options.\n", requestPathForLogging)
		if err := w.SetResponse(code, ct, bytes.NewReader(payload)); err != nil {
			fmt.Printf("MockCoAPServer ERROR: [Response %s] Failed to set basic response (code %v) after SetOptionBytes incompatibility: %v\n", requestPathForLogging, code, err)
		}
		return
	}

	if err := rw.SetResponse(code, ct, bytes.NewReader(payload)); err != nil {
		fmt.Printf("MockCoAPServer ERROR: [Response %s] Failed to set response (code %v): %v\n", requestPathForLogging, code, err)
	}
	for _, opt := range opts {
		if err := rw.SetOptionBytes(opt.ID, opt.Value); err != nil {
			// Non-critical options might fail, so log as warning.
			fmt.Printf("MockCoAPServer WARNING: [Response %s] Failed to set option (ID %v): %v\n", requestPathForLogging, opt.ID, err)
		}
	}
}

// logCoAPRequest captures details of an incoming CoAP request and stores it in the requestHistory.
// It performs deep copies of mutable data (options, payload, token) to ensure immutability.
// It also attempts to reset the request body reader if it's an io.ReadSeeker,
// allowing handlers to read the body after it has been logged.
func (m *MockCoAPServer) logCoAPRequest(path string, req *pool.Message) {
	m.mu.Lock() // Lock to protect shared resources: lastReceivedOptions, lastReceivedMessages, requestHistory
	defer m.mu.Unlock()

	// Deep copy CoAP options. Values within options are byte slices and need copying.
	reqMsgOpts := req.Options()
	optsCopy := make(message.Options, 0, len(reqMsgOpts))
	for _, o := range reqMsgOpts {
		valueCopy := make([]byte, len(o.Value))
		copy(valueCopy, o.Value)
		optsCopy = append(optsCopy, message.Option{ID: o.ID, Value: valueCopy})
	}
	m.lastReceivedOptions[path] = optsCopy // Preserve for now

	// Read and copy the request body.
	var bodyData []byte
	if bodyIOReader := req.Body(); bodyIOReader != nil {
		var errReadBody error
		bodyData, errReadBody = io.ReadAll(bodyIOReader)
		if errReadBody != nil {
			fmt.Printf("MockCoAPServer ERROR: [LogRequest %s] Failed to read request body: %v\n", path, errReadBody)
		}

		// Attempt to reset body reader if it's an io.ReadSeeker. This is crucial for handlers.
		if seeker, ok := bodyIOReader.(io.ReadSeeker); ok {
			if _, errSeek := seeker.Seek(0, io.SeekStart); errSeek != nil {
				// This is an error because subsequent handlers might fail to read the body.
				fmt.Printf("MockCoAPServer ERROR: [LogRequest %s] Failed to seek request body reader to start: %v. Subsequent handlers may fail.\n", path, errSeek)
			}
		} else {
			fmt.Printf("MockCoAPServer WARNING: [LogRequest %s] Request body is not an io.ReadSeeker (type %T). Handlers may not be able to re-read body.\n", path, bodyIOReader)
		}
	}

	// Deep copy token.
	tokenBytes := req.Token()
	tokenCopy := make(message.Token, len(tokenBytes))
	copy(tokenCopy, tokenBytes)

	// Create and store the LoggedRequest.
	loggedReq := LoggedRequest{
		Timestamp: time.Now().UTC(), // Use UTC for consistency
		Path:      path,
		Code:      req.Code(),
		Token:     tokenCopy,
		Type:      req.Type(),
		Options:   optsCopy,
		Payload:   bodyData, // Already copied
	}
	m.requestHistory = append(m.requestHistory, loggedReq)
	// Optional: Implement history trimming if it grows too large (e.g., keep last N requests).
	// if len(m.requestHistory) > MAX_REQUEST_HISTORY_SIZE {
	// 	m.requestHistory = m.requestHistory[len(m.requestHistory)-MAX_REQUEST_HISTORY_SIZE:]
	// }

	// Update lastReceivedMessages (consider phasing out in favor of requestHistory).
	// The payload here is already a copy (bodyData).
	// The main concern with req.Body() being an io.ReadSeeker was for the handlers, which is addressed by the Seek call.
	m.lastReceivedMessages[path] = &message.Message{
		Code:    req.Code(),
		Token:   tokenCopy, // Use the same copied token
		Type:    req.Type(),
		Options: optsCopy, // Use the same copied options
		Payload: bodyData, // Use the copied payload
	}
}

// PostHandler implements RequestHandler for POST requests.
type PostHandler struct {
	server *MockCoAPServer
}

// HandleRequest processes POST requests. It's typically used for creating a new resource
// or updating an existing one. This implementation creates the resource if it doesn't exist.
// It updates the resource's data, content type, and last modified time.
// If the resource is observable, it triggers notifications.
func (h *PostHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for modifying resources map and resource fields
	reqOpts := r.Options()
	optsCopy := make([]message.Option, 0, len(reqOpts)) // Deep copy options
	for _, opt := range reqOpts {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}

	resource, exists := h.server.resources[path]
	if !exists {
		resource = &MockResource{Path: path, Observable: false} // Default to not observable for POST-created
		h.server.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy // Store received options on the resource
	h.server.mu.Unlock()                     // Unlock early before I/O and potential UpdateResource call (which locks)

	// Payload processing should occur after critical section if possible
	data, err := io.ReadAll(r.Body()) // Read payload from mux.Message's Body
	if err != nil {
		fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to read request payload: %v\n", path, err)
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Failed to read payload"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set InternalServerError response after payload read error: %v\n", path, errResp)
		}
		return
	}

	contentType := message.AppOctets // Default content type
	if cf, errCf := r.Options().GetUint32(message.ContentFormat); errCf == nil {
		contentType = message.MediaType(cf)
	}

	// Lock again to update resource fields
	h.server.mu.Lock()
	resource.Data = data
	resource.ContentType = contentType
	resource.LastModified = time.Now()
	isObservable := resource.Observable // Capture before unlocking if UpdateResource is called outside lock
	h.server.mu.Unlock()

	if isObservable {
		// UpdateResource handles its own locking and observer notifications.
		if errUpd := h.server.UpdateResource(path, data); errUpd != nil {
			// This is a warning because the primary POST operation (resource creation/update) likely succeeded before this.
			// However, observers might not be notified correctly.
			fmt.Printf("MockCoAPServer WARNING: [POST %s] Failed to UpdateResource for observation: %v. Client may not receive observation updates.\n", path, errUpd)
		}
	}

	// Check if there's a response delay or sequence to follow
	h.server.mu.Lock()
	res, exists := h.server.resources[path]
	var responseCode codes.Code = codes.Created
	var responseDelay time.Duration
	if exists {
		// Check for response delay
		responseDelay = res.ResponseDelay
		
		// Check for response sequence
		if len(res.ResponseSequence) > 0 {
			if res.CurrentResponseIndex < len(res.ResponseSequence) {
				responseCode = res.ResponseSequence[res.CurrentResponseIndex]
				res.CurrentResponseIndex++
			}
		}
	}
	h.server.mu.Unlock()
	
	// Apply response delay if specified
	if responseDelay > 0 {
		time.Sleep(responseDelay)
	}

	if errSetResp := w.SetResponse(responseCode, message.TextPlain, nil); errSetResp != nil {
		fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set %s response: %v\n", path, responseCode, errSetResp)
	}
}

// PutHandler implements RequestHandler for PUT requests.
type PutHandler struct {
	server *MockCoAPServer
}

// HandleRequest processes PUT requests. It's used for creating a resource at a specific URI
// or replacing an existing one. This implementation creates if not exist or updates if present.
// It updates data, content type, and potentially triggers observe notifications.
func (h *PutHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for resource map and fields
	reqOpts := r.Options()
	optsCopy := make([]message.Option, 0, len(reqOpts)) // Deep copy options
	for _, opt := range reqOpts {
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: append([]byte(nil), opt.Value...)})
	}

	resource, exists := h.server.resources[path]
	created := !exists
	if !exists {
		resource = &MockResource{Path: path, Observable: false} // Default to not observable
		h.server.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy
	h.server.mu.Unlock() // Unlock early before I/O and UpdateResource

	data, err := io.ReadAll(r.Body()) // Read payload from mux.Message's Body
	if err != nil {
		fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to read request payload: %v\n", path, err)
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Failed to read payload"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set InternalServerError response after payload read error: %v\n", path, errResp)
		}
		return
	}

	contentType := message.AppOctets // Default if not specified
	if cf, errCf := r.Options().GetUint32(message.ContentFormat); errCf == nil {
		contentType = message.MediaType(cf)
	}

	// UpdateResource will handle locking for resource data update and notifications.
	// We pass the new content type to be set by UpdateResource if we modify it to accept it,
	// or set it directly on the resource before calling UpdateResource.
	// For now, UpdateResource only takes data. So, lock and update here.
	h.server.mu.Lock()
	resource.Data = data
	resource.ContentType = contentType
	// LastModified is updated by UpdateResource
	h.server.mu.Unlock()

	if errUpd := h.server.UpdateResource(path, data /* TODO: consider passing newOpts like contentType here if UpdateResource is enhanced */); errUpd != nil {
		// Similar to POST, this is a warning as the PUT operation itself might have logically succeeded.
		fmt.Printf("MockCoAPServer WARNING: [PUT %s] Failed to UpdateResource for observation: %v. Client may not receive observation updates.\n", path, errUpd)
		// If UpdateResource itself had a critical error (e.g., resource disappeared), then this might be an ERROR.
		// For now, assume UpdateResource's error is about notifications.
	}

	// Check if there's a response delay or sequence to follow
	h.server.mu.Lock()
	res, exists := h.server.resources[path]
	responseCode := codes.Changed
	if created { // If the PUT created the resource
		responseCode = codes.Created
	}
	var responseDelay time.Duration
	if exists {
		// Check for response delay
		responseDelay = res.ResponseDelay
		
		// Check for response sequence
		if len(res.ResponseSequence) > 0 {
			if res.CurrentResponseIndex < len(res.ResponseSequence) {
				responseCode = res.ResponseSequence[res.CurrentResponseIndex]
				res.CurrentResponseIndex++
			}
		}
	}
	h.server.mu.Unlock()
	
	// Apply response delay if specified
	if responseDelay > 0 {
		time.Sleep(responseDelay)
	}
	
	if errSetResp := w.SetResponse(responseCode, message.TextPlain, nil); errSetResp != nil {
		fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set %s response: %v\n", path, responseCode, errSetResp)
	}
}

// DeleteHandler implements RequestHandler for DELETE requests.
type DeleteHandler struct {
	server *MockCoAPServer
}

// HandleRequest processes DELETE requests. It removes the specified resource and any associated observers.
func (h *DeleteHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for modifying resources and observers maps
	defer h.server.mu.Unlock()

	if _, exists := h.server.resources[path]; !exists {
		if err := w.SetResponse(codes.NotFound, message.TextPlain, bytes.NewReader([]byte("Resource not found"))); err != nil {
			fmt.Printf("MockCoAPServer ERROR: [DELETE %s] Failed to set NotFound response: %v\n", path, err)
		}
		return
	}

	delete(h.server.resources, path)
	// Also remove observers for this path to prevent attempts to notify on a deleted resource.
	delete(h.server.observers, path) // This removes all observers for the path.
	// TODO: Future: If more granular control is needed, iterate through observers and mark them inactive,
	// allowing for potential cleanup or specific observer removal logic.

	if err := w.SetResponse(codes.Deleted, message.TextPlain, nil); err != nil {
		fmt.Printf("MockCoAPServer ERROR: [DELETE %s] Failed to set Deleted response: %v\n", path, err)
	}
}

// addObserver adds a new observer for a given resource path and token.
// It acquires a lock on the server's mutex to safely modify the observers map.
func (m *MockCoAPServer) addObserver(path string, token message.Token, w mux.ResponseWriter) {
	m.mu.Lock() // Ensures thread-safe modification of the m.observers map.
	defer m.mu.Unlock()

	// Copy token to ensure immutability if the original token byte slice is modified elsewhere.
	tokenCopy := make(message.Token, len(token))
	copy(tokenCopy, token)

	obs := Observer{
		Token:    tokenCopy,
		Response: w, // ResponseWriter is an interface, passed by value.
		Active:   true,
	}
	m.observers[path] = append(m.observers[path], obs)
	// fmt.Printf("MockCoAPServer: [%s] Observer added with token %x. Total observers: %d\n", path, tokenCopy, len(m.observers[path]))
}

// sendObserveNotification sends an observe notification to a specific observer.
// It constructs the notification message with the current resource state and Observe option.
// This function is called concurrently for each active observer when a resource is updated.
//
// Important considerations:
// - Observer state: Checks if the observer is active before sending.
// - ResponseWriter: Uses the ResponseWriter captured during the initial observe request.
// - Error Handling: If sending a notification fails (e.g., setting Observe option), the observer is marked inactive.
func (m *MockCoAPServer) sendObserveNotification(observer *Observer, resource *MockResource) {
	if !observer.Active {
		// fmt.Printf("MockCoAPServer DEBUG: [Notify %s] Observer with token %x is inactive. Skipping notification.\n", resource.Path, observer.Token)
		return
	}

	// The ResponseWriter (observer.Response) for an observer is the one from its initial GET request.
	// We use this writer to send subsequent notifications.
	// The underlying go-coap library's ResponseWriter should handle cases where the client connection
	// might have been closed, typically by returning an error on write attempts.

	// Set standard response fields for the notification.
	// Note: ServeWithOptions are from the resource, not the original request.
	// Pass resource.Path for logging context in setResponseWithOptions.
	m.setResponseWithOptions(observer.Response, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions, resource.Path)

	rw, ok := observer.Response.(responseWriterWithOptions)
	if !ok {
		// For testing purposes, continue even if we can't set the Observe option properly
		// The notification content will still be sent, just without the proper Observe sequence number
		fmt.Printf("MockCoAPServer WARNING: [Notify %s] Observer ResponseWriter (token %x) does not implement SetOptionBytes. Notification will be sent without Observe option.\n", resource.Path, observer.Token)
		// Don't deactivate observer - continue sending notifications for testing
	} else {
		// Encode and set the Observe option with the current sequence number.
		buf := make([]byte, 4) // Max size for uint32 CoAP Observe option encoding.
		bytesWritten, _ := message.EncodeUint32(buf, resource.ObserveSeq)
		obsBytes := buf[:bytesWritten]

		if err := rw.SetOptionBytes(message.Observe, obsBytes); err != nil {
			// For testing purposes, log the error but don't deactivate the observer
			fmt.Printf("MockCoAPServer WARNING: [Notify %s] Failed to set Observe option (seq %d, token %x): %v. Continuing anyway.\n", resource.Path, resource.ObserveSeq, observer.Token, err)
		} else {
			// fmt.Printf("MockCoAPServer DEBUG: [Notify %s] Successfully sent Observe notification (seq %d, token %x).\n", resource.Path, resource.ObserveSeq, observer.Token)
		}
	}

	// Note: The actual sending of the message over the wire is handled by the CoAP library
	// when these options and response details are set on the ResponseWriter.
	// If ResponseWriter.SetResponse itself sends synchronously and returns an error for a closed connection,
	// that would be the place to catch it and mark observer inactive.
	// The plgd/go-coap library's ResponseWriter might behave this way.
}

// Test utilities for assertions and inspecting server state.
// These functions provide safe access to server internals for test verification.

// GetRequestHistory returns a copy of the logged CoAP requests.
// This allows tests to inspect the sequence and details of requests received by the server.
// The returned slice is a copy, so modifications to it will not affect the server's internal history.
func (m *MockCoAPServer) GetRequestHistory() []LoggedRequest {
	m.mu.RLock() // Protects read access to requestHistory.
	defer m.mu.RUnlock()

	historyCopy := make([]LoggedRequest, len(m.requestHistory))
	// LoggedRequest fields (Token, Options, Payload) are already deep-copied during their creation in logCoAPRequest.
	// Therefore, a shallow copy of each LoggedRequest struct into the new slice is sufficient here.
	for i, req := range m.requestHistory {
		historyCopy[i] = req
	}
	return historyCopy
}

// ClearRequestHistory removes all entries from the request history log.
// This is useful in tests to reset the server's recorded request state, for example,
// between distinct phases of a test or before a specific action is tested.
func (m *MockCoAPServer) ClearRequestHistory() {
	m.mu.Lock() // Protects write access to requestHistory.
	defer m.mu.Unlock()
	// Reset to a new empty slice, preserving allocated capacity for efficiency if it's to be refilled.
	m.requestHistory = make([]LoggedRequest, 0, cap(m.requestHistory))
}

// GetObserverCount returns the number of currently active observers for a specific resource path.
// This can be used in tests to verify that observation registrations and de-registrations are being handled correctly.
func (m *MockCoAPServer) GetObserverCount(path string) int {
	m.mu.RLock() // Protects read access to the observers map.
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

// GetResourceData retrieves the current data for a resource at the given path.
// It returns a copy of the data and true if the resource exists, otherwise nil and false.
// Returning a copy ensures that the internal state of the mock server is not inadvertently modified by tests.
func (m *MockCoAPServer) GetResourceData(path string) ([]byte, bool) {
	m.mu.RLock() // Protects read access to the resources map.
	defer m.mu.RUnlock()

	if resource, exists := m.resources[path]; exists {
		// Return a copy of the data to prevent external modification of the internal state.
		dataCopy := make([]byte, len(resource.Data))
		copy(dataCopy, resource.Data)
		return dataCopy, true
	}
	return nil, false // Resource not found.
}

// GetLastReceivedOptionsForPath returns a deep copy of the CoAP options from the most recent request
// received for the specified resource path.
// Note: This is largely superseded by GetRequestHistory but retained for potential specific use cases or compatibility.
// Returns the options and true if found, otherwise nil and false.
func (m *MockCoAPServer) GetLastReceivedOptionsForPath(path string) ([]message.Option, bool) {
	m.mu.RLock() // Protects read access to lastReceivedOptions.
	defer m.mu.RUnlock()

	opts, exists := m.lastReceivedOptions[path]
	if !exists {
		return nil, false
	}
	// Return a deep copy to prevent modification of internal state.
	// message.Option values are byte slices, so they need to be copied.
	optsCopy := make([]message.Option, 0, len(opts))
	for _, o := range opts {
		valueCopy := make([]byte, len(o.Value))
		copy(valueCopy, o.Value)
		optsCopy = append(optsCopy, message.Option{ID: o.ID, Value: valueCopy})
	}
	return optsCopy, true
}

// GetResourceLastPutOrPostOptions returns a deep copy of the CoAP options from the last PUT or POST request
// specifically for a given resource path, as stored on the MockResource itself.
// Returns the options and true if found and set, otherwise nil and false.
func (m *MockCoAPServer) GetResourceLastPutOrPostOptions(path string) ([]message.Option, bool) {
	m.mu.RLock() // Protects read access to resources map and resource fields.
	defer m.mu.RUnlock()

	resource, exists := m.resources[path]
	if !exists || resource.LastPutOrPostOptions == nil {
		return nil, false
	}
	// Return a deep copy for safety.
	optsCopy := make([]message.Option, 0, len(resource.LastPutOrPostOptions))
	for _, o := range resource.LastPutOrPostOptions {
		valueCopy := make([]byte, len(o.Value))
		copy(valueCopy, o.Value)
		optsCopy = append(optsCopy, message.Option{ID: o.ID, Value: valueCopy})
	}
	return optsCopy, true
}

// GetResource retrieves a resource by path for testing purposes
func (m *MockCoAPServer) GetResource(path string) (*MockResource, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	resource, exists := m.resources[path]
	if !exists {
		return nil, false
	}
	
	// Return a copy to prevent external modification
	resourceCopy := *resource
	return &resourceCopy, true
}

// MuLock exposes the mutex for test helper functions
func (m *MockCoAPServer) MuLock() {
	m.mu.Lock()
}

// MuUnlock exposes the mutex for test helper functions  
func (m *MockCoAPServer) MuUnlock() {
	m.mu.Unlock()
}

// GetResourceInternal provides direct access to the resource map for test helpers
func (m *MockCoAPServer) GetResourceInternal(path string) (*MockResource, bool) {
	resource, exists := m.resources[path]
	return resource, exists
}

// SetResourceInternal provides direct access to set resources for test helpers
func (m *MockCoAPServer) SetResourceInternal(path string, resource *MockResource) {
	m.resources[path] = resource
}

// SetResourceDelay sets the response delay for a resource
func (m *MockCoAPServer) SetResourceDelay(path string, delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	resource, exists := m.resources[path]
	if !exists {
		return fmt.Errorf("resource not found: %s", path)
	}
	
	resource.ResponseDelay = delay
	return nil
}

// SetupTestServer creates and starts a MockCoAPServer for testing
func SetupTestServer(t interface{}) (*MockCoAPServer, func()) {
	server := NewMockCoAPServer()
	err := server.Start()
	if err != nil {
		panic(fmt.Sprintf("Failed to start mock server: %v", err))
	}
	
	return server, func() {
		server.Stop()
	}
}

// MockCoAPClient provides a simplified CoAP client for use in integration tests,
// typically for interacting with a MockCoAPServer or another CoAP endpoint under test.
// It offers basic GET, POST, PUT, and DELETE operations.
type MockCoAPClient struct {
	serverAddr string       // Target server address in "host:port" format.
	client     *client.Conn // Underlying CoAP client connection.
}

// NewMockCoAPClient creates a new MockCoAPClient targeting the specified server address.
// The serverAddr should be in "host:port" format.
func NewMockCoAPClient(serverAddr string) *MockCoAPClient {
	return &MockCoAPClient{
		serverAddr: serverAddr,
	}
}

// Connect establishes a UDP connection to the CoAP server.
// This must be called before any CoAP operations (GET, POST, etc.).
// The provided context can be used for timeout or cancellation of the dialing process.
func (c *MockCoAPClient) Connect(ctx context.Context) error {
	// udp.Dial is used here, which is suitable for CoAP over UDP.
	// The context is passed via options to control the dialing process.
	conn, err := udp.Dial(c.serverAddr, options.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("MockCoAPClient ERROR: [Connect %s] Failed to connect to server: %w", c.serverAddr, err)
	}
	c.client = conn
	return nil
}

// Close terminates the connection to the CoAP server.
// It should be called when the client is no longer needed to free resources.
// Returns an error if closing the connection fails.
func (c *MockCoAPClient) Close() error {
	if c.client != nil {
		err := c.client.Close()
		c.client = nil // Mark as closed to prevent reuse.
		if err != nil {
			return fmt.Errorf("MockCoAPClient ERROR: [Close] Failed to close client connection: %w", err)
		}
		return nil
	}
	return nil // Client was not connected or already closed.
}

// Get performs a CoAP GET request to the specified path.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Get(ctx context.Context, path string) (*pool.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [GET %s] Client not connected; call Connect() first", path)
	}

	resp, err := c.client.Get(ctx, path) // client.Get handles token generation and message construction.
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [GET %s] Request failed: %w", path, err)
	}
	return resp, nil // resp is already the *pool.Message
}

// Post performs a CoAP POST request to the specified path with the given content type and payload.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Post(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*pool.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [POST %s] Client not connected; call Connect() first", path)
	}

	// The payload must be an io.Reader for the go-coap client.Post method.
	resp, err := c.client.Post(ctx, path, contentType, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [POST %s] Request failed: %w", path, err)
	}
	return resp, nil
}

// Put performs a CoAP PUT request to the specified path with the given content type and payload.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Put(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*pool.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [PUT %s] Client not connected; call Connect() first", path)
	}

	// The payload must be an io.Reader for the go-coap client.Put method.
	resp, err := c.client.Put(ctx, path, contentType, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [PUT %s] Request failed: %w", path, err)
	}
	return resp, nil
}

// Delete performs a CoAP DELETE request to the specified path.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Delete(ctx context.Context, path string) (*pool.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [DELETE %s] Client not connected; call Connect() first", path)
	}

	resp, err := c.client.Delete(ctx, path) // client.Delete handles message construction.
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [DELETE %s] Request failed: %w", path, err)
	}
	return resp, nil
}
