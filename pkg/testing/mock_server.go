// pkg/testing/mock_server.go

// Package testing provides utilities for integration testing of CoAP (Constrained Application Protocol) applications.
//
// The primary component is MockCoAPServer, an in-memory CoAP server designed for configurability
// and simulation of various CoAP resource behaviors, including observability as defined in RFC 7641.
//
// Key Features of MockCoAPServer:
//   - Dynamic Resource Management: Allows defining resources with specific content types,
//     data payloads, and observable characteristics. Resources can be added, updated, or removed at runtime.
//   - Extensible Request Handling: Utilizes a `RequestHandler` interface, enabling customized processing
//     for different CoAP methods (GET, POST, PUT, DELETE). Default handlers are provided for common operations.
//   - Observability Simulation (RFC 7641): Supports CoAP observe requests. Resources can be marked as
//     observable, and updates to such resources will trigger notification messages to registered observers.
//   - Detailed Request Logging: Captures details of incoming CoAP requests (method, path, token, options, payload)
//     into a request history, which can be inspected by tests for assertion purposes. All logged data is deep-copied
//     to ensure immutability and prevent unintended side effects.
//   - Response Customization: Allows configuring response delays and sequences of response codes for resources,
//     facilitating the simulation of various network conditions and server behaviors (e.g., transient errors).
//   - Concurrency-Safe: Designed for safe use in concurrent testing scenarios, with internal state protected by mutexes.
//   - Server State Inspection: Provides methods to query server state, such as the number of active observers
//     for a resource, current resource data, and the history of received requests.
//
// MockCoAPClient:
//   A basic CoAP client is also provided for straightforward interaction with the MockCoAPServer or other
//   CoAP servers within test environments. It simplifies sending requests and receiving responses.
//
// Overall, this package aims to streamline the development of CoAP integration tests by offering
// controllable and introspectable server and client components, thereby reducing reliance on
// external CoAP infrastructure or complex network setups during the testing phase.
package testing

import (
	"bytes" // Required for converting payload []byte to io.Reader for client Post/Put.
// It allows adding resources, simulating CoAP methods (GET, POST, PUT, DELETE),
// and observing resource changes (RFC 7641).
//
// Key features:
//   - Resource Management: Add, update, and delete resources dynamically.
//   - Request Handling: Uses a RequestHandler interface for extensible processing of CoAP methods.
//   - Observability: Supports CoAP observe; resources can be marked as observable, and updates trigger notifications.
//   - Request History: Logs incoming requests (path, method, options, payload) for test assertions. All logged data is deep-copied.
//   - Thread-Safe: Operations are protected by mutexes for safe concurrent access.
//   - Response Customization: Supports configuring response delays and sequences of response codes per resource.
type MockCoAPServer struct {
	addr                 string
	listener             *coapNet.UDPConn // Underlying CoAP UDP connection.
	server               *server.Server                  // The core CoAP server instance.
	resources            map[string]*MockResource        // Map of URI paths to their corresponding MockResource.
	requestHandlers      map[codes.Code]RequestHandler // Map of CoAP codes to their respective handlers.
	observers            map[string][]Observer           // Map of URI paths to lists of active observers.
	lastReceivedOptions  map[string][]message.Option   // DEPRECATED: Use GetRequestHistory() for comprehensive details. Stores options of the last request per path.
	lastReceivedMessages map[string]*message.Message   // DEPRECATED: Use GetRequestHistory() for comprehensive details. Stores the last message per path.
	mu                   sync.RWMutex                    // Mutex for thread-safe access to shared server state (resources, observers, history, etc.).
	running              bool                            // Indicates if the server is currently running and listening for requests.
	cancel               context.CancelFunc              // Function to cancel the server's context, used for gracefully stopping the server.
	requestHistory       []LoggedRequest                 // Log of received requests for test inspection. Each entry is a deep copy.
	// TODO: Consider adding responseHistory for enhanced observability, potentially storing LoggedResponse structs if detailed response logging is needed.
}

// LoggedRequest stores detailed information about a CoAP request received by the MockCoAPServer.
// This struct is used to populate the `requestHistory` for later inspection in tests.
// All mutable fields (Token, Options, Payload) are deep copies of the original request data
// to ensure they are not modified by subsequent processing or by the original sender after the request is logged.
// This immutability is crucial for reliable test assertions.
type LoggedRequest struct {
	Timestamp time.Time         // Timestamp indicating when the server logged the request (UTC).
	Path      string            // URI path extracted from the request (e.g., "/temperature").
	Code      codes.Code        // CoAP method code (e.g., codes.GET, codes.POST, codes.PUT, codes.DELETE).
	Token     message.Token     // CoAP token from the request (a deep copy).
	Type      message.MessageType // CoAP message type (Confirmable, NonConfirmable, Acknowledgement, Reset).
	Options   message.Options   // CoAP options included in the request (a deep copy, including option values).
	Payload   []byte            // Payload/body of the request (a deep copy).
}

// responseWriterWithOptions is an internal interface used to abstract the setting of CoAP options
// on a response writer. This is primarily a workaround for limitations in the underlying
// CoAP library's mux.ResponseWriter interface, which does not directly expose methods like SetOptionBytes.
// This interface allows the MockCoAPServer to leverage such methods if the concrete ResponseWriter type supports them.
type responseWriterWithOptions interface {
	mux.ResponseWriter
	SetOptionBytes(id message.OptionID, value []byte) error
}

// MockResource represents a simulated CoAP resource within the MockCoAPServer.
// It holds the resource's properties (path, content type, data), its observation-related state
// (observable flag, sequence number), and configurations for simulating specific server behaviors
// such as response delays or a predefined sequence of response codes.
//
// Fields:
//   Path: URI path of the resource (e.g., "/sensors/temp").
//   ContentType: CoAP content type for the resource's payload (e.g., message.AppJSON).
//   Data: Current data/payload of the resource.
//   Observable: Boolean flag indicating if the resource supports CoAP observation (RFC 7641).
//   ObserveSeq: Current observe sequence number, incremented on updates if observable.
//   LastModified: Timestamp (UTC) of the last modification.
//   ServeWithOptions: CoAP options (e.g., ETag) included in GET responses for this resource.
//   LastPutOrPostOptions: CoAP options from the last PUT or POST request to this resource, stored for inspection.
//   ResponseDelay: Optional delay applied before sending a response, simulating latency.
//   ResponseSequence: Optional sequence of CoAP codes for successive responses. Cycles through the sequence.
//   currentResponseIndex: Internal index for the ResponseSequence.
type MockResource struct {
	Path                 string            // URI path of the resource (e.g., "/sensors/temp").
	ContentType          message.MediaType   // CoAP content type of the resource's payload (e.g., message.AppJSON).
	Data                 []byte            // Current data/payload of the resource. This is a direct byte slice.
	Observable           bool              // Flag indicating if the resource supports CoAP observation (RFC 7641).
	ObserveSeq           uint32            // Current observe sequence number for notifications. Incremented on updates.
	LastModified         time.Time          // Timestamp of the last modification to the resource (UTC).
	ServeWithOptions     []message.Option   // CoAP options to be included in responses for GET requests to this resource (e.g., ETag).
	LastPutOrPostOptions []message.Option   // CoAP options received in the last PUT or POST request to this resource. Stored for inspection.
	ResponseDelay        time.Duration      // Optional delay to introduce before sending a response, simulating network latency or slow processing.
	ResponseSequence     []codes.Code       // Optional sequence of CoAP codes to respond with for successive requests. Cycles through them.
	currentResponseIndex int                // Internal: tracks the current index for ResponseSequence.
}

// Observer represents an active CoAP observer client for a specific resource within the MockCoAPServer.
// It stores the observer's identifying token and the ResponseWriter associated with the observation relationship,
// which is used to send notifications (updates) back to the client.
// The Active flag indicates if the server should still attempt to send notifications to this observer.
//
// Fields:
//   Token: CoAP token that uniquely identifies the observation relationship. This is a deep copy.
//   Response: The ResponseWriter used for sending notifications to the observer.
//   Active: Flag indicating if the observer is still considered active. Marked false if notifications fail.
type Observer struct {
	Token    message.Token      // CoAP token that uniquely identifies the observation relationship.
	Response mux.ResponseWriter // ResponseWriter for sending notifications (observe updates) to the observer.
	Active   bool               // Flag indicating if the observer is still considered active. Becomes false if notifications fail.
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
// AddResource adds a new resource to the mock server or updates an existing one at the given path.
//
// Parameters:
//   path: The URI path for the resource (e.g., "/temp", "/config/led").
//   contentType: The CoAP message.MediaType to be used for the resource's payload.
//   data: The initial byte slice data for the resource.
//   observable: A boolean indicating whether this resource should support CoAP observations.
//               If true, clients can register to observe changes.
//   opts: A variadic slice of message.Option to be served with this resource on GET requests.
//         These options (e.g., ETag) are added to every GET response for this resource.
func (m *MockCoAPServer) AddResource(path string, contentType message.MediaType, data []byte, observable bool, opts ...message.Option) {
	m.mu.Lock()
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
	listener             *coapNet.UDPConn // Underlying CoAP UDP connection.
	server               *server.Server                  // The core CoAP server instance.
	resources            map[string]*MockResource        // Map of URI paths to their corresponding MockResource.
	requestHandlers      map[codes.Code]RequestHandler // Map of CoAP codes to their respective handlers.
	observers            map[string][]Observer           // Map of URI paths to lists of active observers.
	lastReceivedOptions  map[string][]message.Option   // Stores the options of the last request for a given path (legacy, consider phasing out).
	lastReceivedMessages map[string]*message.Message   // Stores the last message for a given path (legacy, consider phasing out).
	mu                   sync.RWMutex                    // Mutex for thread-safe access to shared server state.
	running              bool                            // Indicates if the server is currently running.
	cancel               context.CancelFunc              // Function to cancel the server's context, used for stopping.
	requestHistory       []LoggedRequest                 // Log of received requests for test inspection.
	// TODO: Consider adding responseHistory for enhanced observability, potentially storing LoggedResponse structs if detailed response logging is needed.
}

// LoggedRequest stores detailed information about a CoAP request received by the mock server.
// This struct is used to populate the request history for later inspection in tests.
// All mutable fields (Token, Options, Payload) are deep copies of the original request data
// to ensure they are not modified by subsequent processing.
type LoggedRequest struct {
	Timestamp time.Time         // Timestamp indicating when the server logged the request (UTC).
	Path      string            // URI path extracted from the request.
	Code      codes.Code        // CoAP method code (e.g., codes.GET, codes.POST).
	Token     message.Token     // CoAP token (a deep copy).
	Type      message.MessageType // CoAP message type (Confirmable, NonConfirmable, etc.).
	Options   message.Options   // CoAP options included in the request (a deep copy).
	Payload   []byte            // Payload/body of the request (a deep copy).
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
	ContentType          message.MediaType   // CoAP content type of the resource's payload.
	Data                 []byte            // Current data/payload of the resource.
	Observable           bool              // Flag indicating if the resource supports CoAP observation.
	ObserveSeq           uint32            // Current observe sequence number for notifications.
	LastModified         time.Time          // Timestamp of the last modification to the resource.
	ServeWithOptions     []message.Option   // CoAP options to be included in responses for GET requests to this resource.
	LastPutOrPostOptions []message.Option   // CoAP options received in the last PUT or POST request to this resource.
	ResponseDelay        time.Duration      // Optional delay to introduce before sending a response.
	ResponseSequence     []codes.Code       // Optional sequence of CoAP codes to respond with. Cycles through them.
	currentResponseIndex int                // Internal: tracks the current index for ResponseSequence.
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
// AddResource adds a new resource to the mock server or updates an existing one at the given path.
//
// Parameters:
//   path: The URI path for the resource (e.g., "/temp", "/config/led").
//   contentType: The CoAP message.MediaType to be used for the resource's payload.
//   data: The initial byte slice data for the resource.
//   observable: A boolean indicating whether this resource should support CoAP observations.
//               If true, clients can register to observe changes.
//   opts: A variadic slice of message.Option to be served with this resource on GET requests.
//         These options are added to every GET response for this resource.
func (m *MockCoAPServer) AddResource(path string, contentType message.MediaType, data []byte, observable bool, opts ...message.Option) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.resources[path] = &MockResource{
		Path:             path,
		ContentType:      contentType,
		Data:             data,
		Observable:       observable,
		ObserveSeq:       0, // Initial sequence number for observations.
		LastModified:     time.Now().UTC(), // Use UTC for consistency.
		ServeWithOptions: opts,
	}
}

// UpdateResource modifies an existing resource's data and optionally its served CoAP options.
// If the resource is observable, this triggers a notification sequence to all active observers.
// The resource's LastModified timestamp and ObserveSeq (if observable) are updated.
//
// Parameters:
//   path: The URI path of the resource to update.
//   data: The new byte slice data for the resource.
//   newOpts: Optional CoAP options that will replace the resource's ServeWithOptions if provided.
//            If not provided, existing ServeWithOptions are retained.
//
// Returns:
//   An error if the resource at the given path is not found. Otherwise, nil.
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
	if len(newOpts) > 0 { // Allow updating options served by the resource too
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
	router.Handle("/", mux.HandlerFunc(m.handleRequest))

	m.server = server.New(options.WithMux(router))

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel // Store cancel function to allow stopping the server.

	go func() {
		// Serve blocks until the listener is closed or an error occurs.
		// Run in a goroutine to not block the Start() method.
		if err := m.server.Serve(listener); err != nil && ctx.Err() == nil {
			// Log errors from the underlying server unless the context was canceled (server stopped intentionally).
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
		m.cancel() // Signal the server's goroutine to exit.
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

// GetHandler implements RequestHandler for CoAP GET requests.
// It retrieves resource data, handles observe registration (RFC 7641),
// and applies any configured response customizations like delays or sequenced error codes.
type GetHandler struct {
	server *MockCoAPServer // Reference to the main server instance for accessing shared state.
}

// HandleRequest processes GET requests. It supports regular GET and observe registration.
// It checks if the resource exists, if it's observable (for observe requests),
// and sends the appropriate response including CoAP options like Observe and ETag (if defined on the MockResource).
// It also incorporates any configured ResponseDelay or ResponseSequence for the resource.
func (h *GetHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock to safely access and potentially update resource.currentResponseIndex.
	resource, exists := h.server.resources[path]

	responseCode := codes.Content // Default success code for GET.
	var responsePayload []byte    // Store raw payload bytes
	var responseOptions []message.Option
	var responseContentType message.MediaType

	if exists {
		if resource.ResponseDelay > 0 {
			time.Sleep(resource.ResponseDelay)
		}
		if len(resource.ResponseSequence) > 0 {
			responseCode = resource.ResponseSequence[resource.currentResponseIndex]
			resource.currentResponseIndex = (resource.currentResponseIndex + 1) % len(resource.ResponseSequence)
		}

		responsePayload = resource.Data
		responseOptions = resource.ServeWithOptions
		responseContentType = resource.ContentType
	} else {
		responseCode = codes.NotFound
		responseContentType = message.TextPlain
		responsePayload = []byte("Resource not found")
	}
	h.server.mu.Unlock() // Unlock after accessing/modifying resource state

	if !exists { // Handle not found case after unlock
		// Use bytes.NewReader for payload when calling SetResponse for consistency
		if err := w.SetResponse(responseCode, responseContentType, bytes.NewReader(responsePayload)); err != nil {
			fmt.Printf("MockCoAPServer ERROR: [GET %s] Failed to set %s response: %v\n", path, responseCode, err)
		}
		return
	}

	// If we are here, resource exists.
	// If the responseCode determined by ResponseSequence is an error code, send it directly.
	if responseCode >= codes.BadRequest {
		errorMsg := responsePayload
		if len(errorMsg) == 0 { // Default error payload if resource.Data is empty for an error code
			errorMsg = []byte(fmt.Sprintf("GET failed with server configured error: %s", responseCode.String()))
		}
		// Use the original resource content type if available, otherwise default to text/plain for errors
		ct := responseContentType
		if ct == 0 { // Assuming 0 is not a valid explicit content type / means not set. plgd-dev/go-coap uses 0 as a sentinel for no format.
			ct = message.TextPlain
		}
		h.server.setResponseWithOptions(w, responseCode, ct, errorMsg, responseOptions, path)
		return
	}

	// Proceed with observe logic or regular GET if success code
	reqOpts := r.Options()
	if observeVal, err := reqOpts.GetUint32(message.Observe); err == nil && observeVal == 0 { // Observe registration
		if resource.Observable {
			h.server.addObserver(path, r.Token(), w)
			h.server.setResponseWithOptions(w, responseCode, responseContentType, responsePayload, responseOptions, path)

			rw, ok := w.(responseWriterWithOptions)
			if !ok {
				// This is a warning because the main content response is still sent.
				// However, the client won't get the observe sequence number on this initial response.
				fmt.Printf("MockCoAPServer WARNING: [GET %s] ResponseWriter does not implement SetOptionBytes; cannot set Observe option on initial response.\n", path)
				return // Return after sending content, as Observe option cannot be set.
			}

			buf := make([]byte, 4) // Max size for uint32 CoAP encoding
			resultBuffer, bytesWritten := message.EncodeUint32(buf, resource.ObserveSeq)
			obsBytes := resultBuffer[:bytesWritten]
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
	} else { // Regular GET or GET for already observing client (no observe option in this request)
		// For regular GET, payload is resource.Data.
		h.server.setResponseWithOptions(w, responseCode, responseContentType, responsePayload, responseOptions, path)
	}
}

// setResponseWithOptions is a helper utility to set the response code, content type, payload,
// and any additional CoAP options (like ETag or Max-Age from MockResource.ServeWithOptions)
// on the ResponseWriter. It centralizes response sending logic and includes error logging.
// The 'requestPathForLogging' parameter provides context for log messages.
// Payload should be a byte slice; it will be wrapped in an io.Reader internally if needed by SetResponse.
func (m *MockCoAPServer) setResponseWithOptions(w mux.ResponseWriter, code codes.Code, ct message.MediaType, payload []byte, opts []message.Option, requestPathForLogging string) {
	rw, ok := w.(responseWriterWithOptions)
	if !ok {
		// If the ResponseWriter doesn't support setting options directly, log a warning and send a basic response.
		fmt.Printf("MockCoAPServer WARNING: [Response %s] ResponseWriter does not implement SetOptionBytes. Cannot set custom CoAP options from ServeWithOptions.\n", requestPathForLogging)
		// For plgd-dev/go-coap, SetResponse takes an io.ReadSeeker for the payload.
		var payloadReader io.ReadSeeker
		if payload != nil {
			payloadReader = bytes.NewReader(payload)
		}
		if err := w.SetResponse(code, ct, payloadReader); err != nil {
			fmt.Printf("MockCoAPServer ERROR: [Response %s] Failed to set basic response (code %v) after SetOptionBytes incompatibility: %v\n", requestPathForLogging, code, err)
		}
		return
	}

	// For plgd-dev/go-coap, SetResponse takes an io.ReadSeeker for the payload.
	var payloadReader io.ReadSeeker
	if payload != nil {
		payloadReader = bytes.NewReader(payload)
	}
	if err := rw.SetResponse(code, ct, payloadReader); err != nil {
		fmt.Printf("MockCoAPServer ERROR: [Response %s] Failed to set response (code %v): %v\n", requestPathForLogging, code, err)
		// Even if SetResponse fails, attempt to set options if they are critical (e.g. Observe for notifications)
		// For general options from ServeWithOptions, this might be less critical.
	}

	// Set any additional options specified (e.g., in MockResource.ServeWithOptions).
	for _, opt := range opts {
		if err := rw.SetOptionBytes(opt.ID, opt.Value); err != nil {
			// Non-critical options might fail (e.g., client doesn't support an option), so log as warning.
			fmt.Printf("MockCoAPServer WARNING: [Response %s] Failed to set option (ID %v, Value %x): %v\n", requestPathForLogging, opt.ID, opt.Value, err)
		}
	}
}

// logCoAPRequest captures details of an incoming CoAP request and stores it in the requestHistory.
// It performs deep copies of mutable data structures within the request (Options, Payload, Token)
// to ensure that the logged information is immutable and safe from modifications by subsequent processing
// or by the original sender. This is crucial for reliable test assertions based on request history.
//
// The method also attempts to reset the request body reader if it's an io.ReadSeeker. This allows
// the actual CoAP handlers to read the body after it has been read and logged here. If the body
// cannot be reset, a warning is logged, as handlers might not be able to access the payload.
func (m *MockCoAPServer) logCoAPRequest(path string, req *pool.Message) {
	m.mu.Lock() // Lock to protect shared resources: lastReceivedOptions, lastReceivedMessages, requestHistory.
	defer m.mu.Unlock()

	// Deep copy CoAP options: message.Options is a slice of message.Option.
	// Each message.Option contains a Value ([]byte) which must be copied.
	// This ensures that modifications to option values elsewhere do not affect the logged request.
	reqMsgOpts := req.Options()
	optsCopy := make(message.Options, 0, len(reqMsgOpts))
	for _, o := range reqMsgOpts {
		valueCopy := make([]byte, len(o.Value))
		copy(valueCopy, o.Value) // Deep copy of the option's value.
		optsCopy = append(optsCopy, message.Option{ID: o.ID, Value: valueCopy})
	}
	m.lastReceivedOptions[path] = optsCopy // DEPRECATED: Retained for now.

	// Read and deep copy the request body. io.ReadAll creates a new []byte slice,
	// effectively making a deep copy of the payload data.
	var bodyData []byte
	if bodyIOReader := req.Body(); bodyIOReader != nil {
		var errReadBody error
		bodyData, errReadBody = io.ReadAll(bodyIOReader) // bodyData is a new slice, effectively a deep copy.
		if errReadBody != nil {
			fmt.Printf("MockCoAPServer ERROR: [LogRequest %s] Failed to read request body for logging: %v\n", path, errReadBody)
			// bodyData will be nil or partially read; this is acceptable for logging the attempt.
		}

		// Attempt to reset the body reader if it's an io.ReadSeeker. This is crucial for subsequent handlers
		// to be able to read the body as well. If the body was read by io.ReadAll, the original reader is now at EOF.
		if seeker, ok := bodyIOReader.(io.ReadSeeker); ok {
			if _, errSeek := seeker.Seek(0, io.SeekStart); errSeek != nil {
				// This is a significant issue as subsequent handlers might fail to read the body.
				fmt.Printf("MockCoAPServer ERROR: [LogRequest %s] Failed to seek request body reader to start after reading for logging: %v. Subsequent CoAP handlers may fail to read payload.\n", path, errSeek)
			}
		} else {
			// If not a ReadSeeker, the body has been consumed by logging, and handlers cannot re-read it.
			// This might be acceptable for some tests but can lead to unexpected behavior if handlers expect to read the body.
			fmt.Printf("MockCoAPServer WARNING: [LogRequest %s] Request body is not an io.ReadSeeker (type %T). Body was consumed by logging; CoAP handlers may not be able to re-read payload.\n", path, bodyIOReader)
		}
	}

	// Deep copy token: message.Token is a []byte.
	// This ensures that modifications to the token elsewhere do not affect the logged request.
	tokenBytes := req.Token()
	tokenCopy := make(message.Token, len(tokenBytes))
	copy(tokenCopy, tokenBytes) // Deep copy of the token.

	// Create and store the LoggedRequest. All fields containing mutable data (Token, Options, Payload)
	// are now populated with deep copies.
	loggedReq := LoggedRequest{
		Timestamp: time.Now().UTC(), // Use UTC for consistency.
		Path:      path,
		Code:      req.Code(),
		Token:     tokenCopy,    // Deep-copied token.
		Type:      req.Type(),
		Options:   optsCopy,     // Deep-copied options.
		Payload:   bodyData,     // Deep-copied payload.
	}
	m.requestHistory = append(m.requestHistory, loggedReq)

	// Optional: Implement history trimming if it grows too large (e.g., keep last N requests).
	// Example:
	// const MAX_REQUEST_HISTORY_SIZE = 200 // Define a reasonable maximum.
	// if len(m.requestHistory) > MAX_REQUEST_HISTORY_SIZE {
	//   m.requestHistory = m.requestHistory[len(m.requestHistory)-MAX_REQUEST_HISTORY_SIZE:]
	// }

	// Update lastReceivedMessages (DEPRECATED: consider phasing out in favor of requestHistory).
	// The payload here (bodyData) is already a deep copy.
	// The main concern with req.Body() being an io.ReadSeeker was for the handlers, which is addressed by the Seek call attempt.
	m.lastReceivedMessages[path] = &message.Message{ // This structure is part of the public API via GetLastReceivedMessage.
		Code:    req.Code(),
		Token:   tokenCopy, // Use the same deep-copied token.
		Type:    req.Type(),
		Options: optsCopy,  // Use the same deep-copied options.
		Payload: bodyData,  // Use the deep-copied payload.
	}
}

// PostHandler implements RequestHandler for CoAP POST requests.
// It typically handles resource creation or updates. This implementation creates the resource
// if it doesn't exist, or updates it if it does. It updates the resource's data, content type,
// and last modified time. If the resource is observable, it triggers notifications.
// ResponseDelay and ResponseSequence from the MockResource are also respected.
type PostHandler struct {
	server *MockCoAPServer // Reference to the main server instance.
}

// HandleRequest processes POST requests. It's typically used for creating a new resource
// or updating an existing one. This implementation creates the resource if it doesn't exist.
// It updates the resource's data, content type (from request's ContentFormat option), and last modified time.
// If the resource is observable, this will trigger notifications via UpdateResource.
// It also incorporates any configured ResponseDelay or ResponseSequence for the resource.
func (h *PostHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for modifying resources map and resource fields.
	reqOpts := r.Options()
	// Deep copy options received with the POST request to store on the resource.
	optsCopy := make([]message.Option, 0, len(reqOpts))
	for _, opt := range reqOpts {
		valueCopy := make([]byte, len(opt.Value))
		copy(valueCopy, opt.Value)
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: valueCopy})
	}

	resource, exists := h.server.resources[path]
	if !exists {
		// If resource doesn't exist, create a new one. By default, POST-created resources are not observable
		// unless explicitly made so later via AddResource or direct manipulation for testing.
		resource = &MockResource{Path: path, Observable: false}
		h.server.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy // Store deep-copied received options on the resource.

	// Determine response code, applying ResponseDelay and ResponseSequence.
	responseCode := codes.Created // Default for POST success (resource created or updated).
	// Per RFC 7252, POST can also result in 2.04 (Changed) if the resource already existed and was updated.
	// Here, we simplify to 2.01 (Created) for both creation and update via POST,
	// unless overridden by ResponseSequence.
	if resource.ResponseDelay > 0 {
		time.Sleep(resource.ResponseDelay)
	}
	if len(resource.ResponseSequence) > 0 {
		// Override default responseCode if a sequence is defined for this resource.
		responseCode = resource.ResponseSequence[resource.currentResponseIndex]
		resource.currentResponseIndex = (resource.currentResponseIndex + 1) % len(resource.ResponseSequence)
	}
	h.server.mu.Unlock() // Unlock after initial resource access/creation and response logic setup.

	// Payload processing. This should be done after the lock for initial resource setup is released,
	// as payload reading can be an I/O operation.
	data, err := io.ReadAll(r.Body()) // Read payload from mux.Message's Body.
	if err != nil {
		fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to read request payload: %v\n", path, err)
		// Use bytes.NewReader for consistent payload handling in SetResponse.
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Failed to read payload"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set InternalServerError response after payload read error: %v\n", path, errResp)
		}
		return
	}

	// Determine content type from request options.
	contentType := message.AppOctets // Default content type if not specified.
	if cf, errCf := r.Options().GetUint32(message.ContentFormat); errCf == nil {
		contentType = message.MediaType(cf)
	}

	// Lock again to update resource fields with new data and content type.
	h.server.mu.Lock()
	// Re-fetch resource in case it was modified between locks (though unlikely for POST to same path by same handler).
	// More relevant if other goroutines could modify resources. For simplicity, assume `resource` pointer is still valid.
	if res, ok := h.server.resources[path]; ok { // Ensure resource still exists if it was created above.
		res.Data = data
		res.ContentType = contentType
		// LastModified and ObserveSeq are updated by UpdateResource if observable.
		// If not observable, we should update LastModified here.
		if !res.Observable {
			res.LastModified = time.Now().UTC()
		}
		isObservable := res.Observable
		h.server.mu.Unlock() // Unlock before potentially calling UpdateResource (which locks internally).

		if isObservable {
			// UpdateResource handles its own locking and observer notifications.
			// It will update LastModified and ObserveSeq.
			if errUpd := h.server.UpdateResource(path, data); errUpd != nil {
				// This is a warning because the primary POST operation (resource creation/update) likely succeeded.
				// However, observers might not be notified correctly.
				fmt.Printf("MockCoAPServer WARNING: [POST %s] Failed to UpdateResource for observation: %v. Client may not receive observation updates.\n", path, errUpd)
			}
		}
	} else {
		// Should not happen if resource was created above and map wasn't modified externally.
		h.server.mu.Unlock()
		fmt.Printf("MockCoAPServer ERROR: [POST %s] Resource disappeared unexpectedly after creation. Cannot update with payload.\n", path)
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Internal server error processing POST"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set InternalServerError response after resource disappearance: %v\n", path, errResp)
		}
		return
	}


	// Respond based on determined responseCode (from ResponseSequence or default).
	if responseCode < codes.BadRequest { // Success codes like 2.01 Created or 2.04 Changed.
		// Payload for successful POST is often empty or a representation of the new resource/status.
		// For simplicity, send empty payload for success.
		if errSetResp := w.SetResponse(responseCode, message.TextPlain, nil); errSetResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set %s response: %v\n", path, responseCode, errSetResp)
		}
	} else { // Error codes determined by ResponseSequence.
		errorPayload := []byte(fmt.Sprintf("POST failed with server configured error: %s", responseCode.String()))
		if errSetResp := w.SetResponse(responseCode, message.TextPlain, bytes.NewReader(errorPayload)); errSetResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [POST %s] Failed to set %s error response: %v\n", path, responseCode, errSetResp)
		}
	}
}

// PutHandler implements RequestHandler for CoAP PUT requests.
// It's used for creating a resource at a specific URI or replacing/updating an existing one.
// This implementation creates the resource if it doesn't exist or updates it if it's present.
// It updates data, content type (from request's ContentFormat), and potentially triggers observe notifications.
// ResponseDelay and ResponseSequence from the MockResource are respected.
type PutHandler struct {
	server *MockCoAPServer // Reference to the main server instance.
}

// HandleRequest processes PUT requests. It's used for creating a resource at a specific URI
// or replacing an existing one. This implementation creates if not exist or updates if present.
// It updates data, content type (from ContentFormat option), and potentially triggers observe notifications
// if the resource is observable.
// It also incorporates any configured ResponseDelay or ResponseSequence for the resource.
func (h *PutHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for resource map and fields.
	reqOpts := r.Options()
	// Deep copy options received with the PUT request.
	optsCopy := make([]message.Option, 0, len(reqOpts))
	for _, opt := range reqOpts {
		valueCopy := make([]byte, len(opt.Value))
		copy(valueCopy, opt.Value)
		optsCopy = append(optsCopy, message.Option{ID: opt.ID, Value: valueCopy})
	}

	resource, exists := h.server.resources[path]
	created := !exists // Will be true if resource is created by this PUT.
	if !exists {
		// If resource doesn't exist, create a new one. By default, PUT-created resources are not observable
		// unless explicitly made so later.
		resource = &MockResource{Path: path, Observable: false}
		h.server.resources[path] = resource
	}
	resource.LastPutOrPostOptions = optsCopy // Store deep-copied received options.

	// Determine response code, applying ResponseDelay and ResponseSequence.
	responseCode := codes.Changed // Default for PUT update (resource existed).
	if created {
		responseCode = codes.Created // Default for PUT create (resource did not exist).
	}

	if resource.ResponseDelay > 0 {
		time.Sleep(resource.ResponseDelay)
	}
	if len(resource.ResponseSequence) > 0 {
		// Override default responseCode if a sequence is defined.
		responseCode = resource.ResponseSequence[resource.currentResponseIndex]
		resource.currentResponseIndex = (resource.currentResponseIndex + 1) % len(resource.ResponseSequence)
	}
	h.server.mu.Unlock() // Unlock after initial resource access/creation and response logic setup.

	// Payload processing - similar to POST handler.
	data, err := io.ReadAll(r.Body()) // Read payload from mux.Message's Body.
	if err != nil {
		fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to read request payload: %v\n", path, err)
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Failed to read payload"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set InternalServerError response after payload read error: %v\n", path, errResp)
		}
		return
	}

	// Determine content type from request options.
	contentType := message.AppOctets // Default content type if not specified.
	if cf, errCf := r.Options().GetUint32(message.ContentFormat); errCf == nil {
		contentType = message.MediaType(cf)
	}

	// Update resource state. This needs locking.
	// UpdateResource handles its own locking for data update and notifications.
	// However, we need to set ContentType here if it's derived from the PUT request.
	// If UpdateResource were enhanced to take contentType, that would be cleaner.
	h.server.mu.Lock()
	// Re-fetch resource to ensure it's still valid.
	if res, ok := h.server.resources[path]; ok {
		res.ContentType = contentType // Set content type from PUT request.
		// Data update and observable notifications are handled by UpdateResource.
		// If not observable, LastModified should be updated here or by UpdateResource (currently UpdateResource updates it).
		h.server.mu.Unlock() // Unlock before calling UpdateResource.

		if errUpd := h.server.UpdateResource(path, data /* newOpts for resource.ServeWithOptions could be passed here */); errUpd != nil {
			// This could be a warning if the PUT operation itself logically succeeded (resource created/data set)
			// but observation notification failed. Or an error if UpdateResource failed critically.
			fmt.Printf("MockCoAPServer WARNING: [PUT %s] UpdateResource call failed or had issues: %v. Client may not receive observation updates or resource state might be inconsistent.\n", path, errUpd)
			// Depending on UpdateResource's error semantics, might need to adjust responseCode here.
			// For now, assume responseCode determined earlier still applies unless UpdateResource indicates total failure.
		}
	} else {
		h.server.mu.Unlock()
		fmt.Printf("MockCoAPServer ERROR: [PUT %s] Resource disappeared unexpectedly. Cannot update with payload.\n", path)
		if errResp := w.SetResponse(codes.InternalServerError, message.TextPlain, bytes.NewReader([]byte("Internal server error processing PUT"))); errResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set InternalServerError response after resource disappearance: %v\n", path, errResp)
		}
		return
	}

	// Respond based on determined responseCode.
	if responseCode < codes.BadRequest { // Success codes (Created, Changed).
		// Successful PUT responses usually have no payload.
		if errSetResp := w.SetResponse(responseCode, message.TextPlain, nil); errSetResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set %s response: %v\n", path, responseCode, errSetResp)
		}
	} else { // Error codes (e.g., from ResponseSequence).
		errorPayload := []byte(fmt.Sprintf("PUT failed with server configured error: %s", responseCode.String()))
		if errSetResp := w.SetResponse(responseCode, message.TextPlain, bytes.NewReader(errorPayload)); errSetResp != nil {
			fmt.Printf("MockCoAPServer ERROR: [PUT %s] Failed to set %s error response: %v\n", path, responseCode, errSetResp)
		}
	}
}

// DeleteHandler implements RequestHandler for CoAP DELETE requests.
// It removes the specified resource from the server and also removes any associated observers for that path.
// ResponseDelay and ResponseSequence from the MockResource are respected.
type DeleteHandler struct {
	server *MockCoAPServer // Reference to the main server instance.
}

// HandleRequest processes DELETE requests. It removes the specified resource and any associated observers.
// It also incorporates any configured ResponseDelay or ResponseSequence for the resource
// to determine the final response code (e.g., simulating delayed deletion or error conditions).
func (h *DeleteHandler) HandleRequest(w mux.ResponseWriter, r *mux.Message, path string) {
	h.server.mu.Lock() // Lock for modifying resources and observers maps.

	resource, exists := h.server.resources[path]
	responseCode := codes.Deleted // Default for successful DELETE.

	if exists {
		// Apply ResponseDelay and ResponseSequence if the resource exists.
		if resource.ResponseDelay > 0 {
			time.Sleep(resource.ResponseDelay)
		}
		if len(resource.ResponseSequence) > 0 {
			responseCode = resource.ResponseSequence[resource.currentResponseIndex]
			resource.currentResponseIndex = (resource.currentResponseIndex + 1) % len(resource.ResponseSequence)
		}
	} else {
		// If resource does not exist, the response is NotFound.
		// ResponseDelay/Sequence from a non-existent resource are not applicable.
		responseCode = codes.NotFound
	}

	// Perform deletion only if the effective responseCode (after considering ResponseSequence) is Deleted.
	// This allows simulating scenarios where a DELETE might be requested but results in an error due to server policy.
	if responseCode == codes.Deleted {
		delete(h.server.resources, path)
		delete(h.server.observers, path) // Also remove any observers for this path.
	}
	h.server.mu.Unlock()


	// Respond based on the determined responseCode.
	// Payload is typically empty for DELETE success or NotFound.
	// For other errors (from ResponseSequence), a descriptive payload might be included.
	var responsePayloadReader io.ReadSeeker
	responseContentType := message.TextPlain // Default content type for error payloads.

	if responseCode == codes.NotFound {
		responsePayloadReader = bytes.NewReader([]byte("Resource not found"))
	} else if responseCode >= codes.BadRequest && responseCode != codes.Deleted { // Other errors (excluding successful Deleted)
		// Provide a generic error payload if one is configured via ResponseSequence.
		responsePayloadReader = bytes.NewReader([]byte(fmt.Sprintf("DELETE operation resulted in error: %s", responseCode.String())))
	}
	// For codes.Deleted, responsePayloadReader remains nil (no body).

	if err := w.SetResponse(responseCode, responseContentType, responsePayloadReader); err != nil {
		fmt.Printf("MockCoAPServer ERROR: [DELETE %s] Failed to set %s response: %v\n", path, responseCode, err)
	}
}

// addObserver adds a new observer for a given resource path and token.
// It acquires a lock on the server's mutex to safely modify the `observers` map.
// The observer's ResponseWriter (captured from the initial GET request) is stored
// to send future notifications. The token is deep-copied for immutability.
func (m *MockCoAPServer) addObserver(path string, token message.Token, w mux.ResponseWriter) {
	m.mu.Lock() // Ensures thread-safe modification of the m.observers map.
	defer m.mu.Unlock()

	// Deep copy token to ensure immutability, as the original token byte slice
	// from the request might be reused or modified elsewhere.
	tokenCopy := make(message.Token, len(token))
	copy(tokenCopy, token)

	obs := Observer{
		Token:    tokenCopy,
		Response: w,    // ResponseWriter is an interface, typically a pointer type, passed by value.
		Active:   true, // New observers are initially active.
	}
	m.observers[path] = append(m.observers[path], obs)
	// fmt.Printf("MockCoAPServer DEBUG: [AddObserver %s] Observer added with token %x. Total observers for path: %d\n", path, tokenCopy, len(m.observers[path]))
}

// sendObserveNotification sends an observe notification to a specific observer.
// It constructs the notification message with the current resource state (data, content type)
// and includes the Observe option with the updated sequence number from the resource.
// This function is typically called concurrently for each active observer when an observed resource is updated.
//
// Key operational aspects:
//   - Activity Check: Verifies if the observer is still marked `Active` before attempting to send.
//     If not active, the function returns early, preventing further processing for this observer.
//   - ResponseWriter Usage: Utilizes the `ResponseWriter` captured during the observer's initial registration (GET request).
//     This writer is the dedicated channel for sending notifications back to that specific observer.
//   - Error Handling and Deactivation: If sending the notification fails at critical stages
//     (e.g., the ResponseWriter doesn't support setting necessary CoAP options, or setting the Observe option itself fails),
//     the observer is marked as inactive (`Active = false`). This prevents further attempts to send notifications
//     to a non-responsive or incorrectly configured observer, thus maintaining a clean list of active observers.
//     Error messages are logged with `MockCoAPServer ERROR:` prefix for such critical failures.
func (m *MockCoAPServer) sendObserveNotification(observer *Observer, resource *MockResource) {
	// Do not attempt to send to an observer that has already been marked as inactive.
	if !observer.Active {
		// fmt.Printf("MockCoAPServer DEBUG: [Notify %s] Observer with token %x is inactive. Skipping notification.\n", resource.Path, observer.Token)
		return
	}

	// The ResponseWriter (observer.Response) for an observer is the one from its initial GET request.
	// This writer is used to send subsequent notifications.
	// The underlying go-coap library's ResponseWriter should handle errors if the client connection
	// has been closed (e.g., by returning an error on write attempts), which would then be caught here.

	// Set standard response fields for the notification (Content code, ContentType, Payload).
	// ServeWithOptions from the resource (e.g., ETag) are also applied to the notification.
	// resource.Path is passed for logging context within setResponseWithOptions.
	// resource.Data is a direct byte slice; setResponseWithOptions will wrap it in an io.ReadSeeker as needed.
	m.setResponseWithOptions(observer.Response, codes.Content, resource.ContentType, resource.Data, resource.ServeWithOptions, resource.Path)

	// Attempt to cast ResponseWriter to responseWriterWithOptions to set the Observe option.
	// This is necessary because the standard mux.ResponseWriter interface doesn't expose SetOptionBytes.
	rw, ok := observer.Response.(responseWriterWithOptions)
	if !ok {
		// This is a critical failure for an observer, as the Observe option cannot be set,
		// making the notification incomplete or non-compliant from the client's perspective.
		// Deactivate the observer to prevent further malformed notifications.
		fmt.Printf("MockCoAPServer ERROR: [Notify %s] Observer ResponseWriter (token %x) does not implement SetOptionBytes. Notification will be incomplete. Deactivating observer.\n", resource.Path, observer.Token)
		observer.Active = false // Deactivate observer due to incompatible ResponseWriter.
		return
	}

	// Encode and set the Observe option with the current sequence number from the resource.
	// This sequence number is crucial for the client to correctly order and process observation updates.
	buf := make([]byte, 4) // Max size for uint32 CoAP Observe option encoding.
	resultBuffer, bytesWritten := message.EncodeUint32(buf, resource.ObserveSeq)
	obsBytes := resultBuffer[:bytesWritten]

	if err := rw.SetOptionBytes(message.Observe, obsBytes); err != nil {
		// Failure to set the Observe option is critical for the client's ability to correctly process the notification.
		// Deactivate the observer as it's not processing these essential parts of notifications correctly.
		fmt.Printf("MockCoAPServer ERROR: [Notify %s] Failed to set Observe option (seq %d, token %x) on notification: %v. Deactivating observer.\n", resource.Path, resource.ObserveSeq, observer.Token, err)
		observer.Active = false // Deactivate observer due to failure in setting Observe option.
	} else {
		// fmt.Printf("MockCoAPServer DEBUG: [Notify %s] Successfully sent Observe notification (seq %d, token %x).\n", resource.Path, resource.ObserveSeq, observer.Token)
	}

	// Note on implicit error handling: The actual sending of the message over the network is handled by the CoAP library
	// when these options and response details are set on the ResponseWriter.
	// If ResponseWriter.SetResponse or SetOptionBytes itself returns an error (e.g., due to a closed connection),
	// that would be an implicit way to detect a non-responsive client. The current logic deactivates observers
	// primarily on failures to *construct* the notification correctly (missing option capabilities) or explicitly
	// failing to set the Observe option. Connection-related errors during the actual transmission by the underlying
	// library might not be caught here unless they propagate up through SetResponse/SetOptionBytes calls.
}

// Test utilities for assertions and inspecting server state.
// These functions provide safe, read-only access to server internals for test verification purposes.

// GetRequestHistory returns a copy of the logged CoAP requests.
// This allows tests to inspect the sequence and details of requests received by the server.
// The returned slice is a deep copy with respect to the LoggedRequest structs it contains,
// meaning modifications to the returned slice or its elements will not affect the server's internal history.
// Each LoggedRequest within the slice also contains deep copies of its mutable fields (Token, Options, Payload),
// ensuring immutability of the historical data.
func (m *MockCoAPServer) GetRequestHistory() []LoggedRequest {
	m.mu.RLock() // Protects read access to requestHistory.
	defer m.mu.RUnlock()

	// Create a new slice and copy LoggedRequest items.
	historyCopy := make([]LoggedRequest, len(m.requestHistory))
	// Each LoggedRequest is a struct. When copied, its fields are copied.
	// Since Token, Options, and Payload within LoggedRequest are already deep copies
	// (made during logCoAPRequest), a direct copy of the LoggedRequest struct
	// effectively preserves the deep-copy characteristic for the elements in historyCopy.
	for i, req := range m.requestHistory {
		historyCopy[i] = req
	}
	return historyCopy
}

// ClearRequestHistory removes all entries from the request history log.
// This is useful in tests to reset the server's recorded request state, for example,
// between distinct test cases or phases within a single test, ensuring that assertions
// are made only against relevant requests.
func (m *MockCoAPServer) ClearRequestHistory() {
	m.mu.Lock() // Protects write access to requestHistory.
	defer m.mu.Unlock()
	// Reset to a new empty slice. The previous slice's capacity might be retained by the runtime
	// if `cap(m.requestHistory)` was passed, which can be efficient if it's refilled soon.
	m.requestHistory = make([]LoggedRequest, 0, cap(m.requestHistory))
}

// GetObserverCount returns the number of currently active observers for a specific resource path.
// This can be used in tests to verify that CoAP observation registrations and de-registrations
// are being handled correctly by the server (e.g., observer count increases on registration,
// decreases on de-registration or when an observer becomes inactive).
// An observer is considered active if its `Active` flag is true.
func (m *MockCoAPServer) GetObserverCount(path string) int {
	m.mu.RLock() // Protects read access to the observers map.
	defer m.mu.RUnlock()

	count := 0
	if observersForPath, exists := m.observers[path]; exists {
		for _, obs := range observersForPath {
			if obs.Active { // Only count observers explicitly marked as active.
				count++
			}
		}
	}
	return count
}

// GetResourceData retrieves the current data (payload) for a resource at the given path.
// It returns a deep copy of the data and true if the resource exists, otherwise nil and false.
// Returning a deep copy ensures that the internal state of the MockCoAPServer is not inadvertently
// modified by tests if the returned byte slice is mutated by the caller.
func (m *MockCoAPServer) GetResourceData(path string) ([]byte, bool) {
	m.mu.RLock() // Protects read access to the resources map.
	defer m.mu.RUnlock()

	if resource, exists := m.resources[path]; exists {
		// Return a deep copy of the data to prevent external modification of the internal state.
		dataCopy := make([]byte, len(resource.Data))
		copy(dataCopy, resource.Data)
		return dataCopy, true
	}
	return nil, false // Resource not found.
}

// GetLastReceivedOptionsForPath returns a deep copy of the CoAP options from the most recent request
// received for the specified resource path, as stored in the legacy `lastReceivedOptions` map.
//
// DEPRECATED: Prefer using `GetRequestHistory()` for more comprehensive request details, as it provides
// access to all logged requests, not just the last one's options. This method is retained primarily
// for backward compatibility or very specific, simple use cases.
//
// Returns the options (as a deep copy) and true if found for the path, otherwise nil and false.
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

// GetResourceLastPutOrPostOptions returns a deep copy of the CoAP options that were part of the
// last PUT or POST request successfully processed for a given resource path. These options are
// stored on the `MockResource` itself (in `MockResource.LastPutOrPostOptions`).
// This is useful for verifying that specific options sent in a state-changing request (like custom
// options or conditional request options such as If-Match) were correctly received and processed by the server.
// Returns the options (as a deep copy) and true if the resource exists and options were set, otherwise nil and false.
func (m *MockCoAPServer) GetResourceLastPutOrPostOptions(path string) ([]message.Option, bool) {
	m.mu.RLock() // Protects read access to resources map and resource fields.
	defer m.mu.RUnlock()

	resource, exists := m.resources[path]
	if !exists || resource.LastPutOrPostOptions == nil {
		// Resource doesn't exist, or no PUT/POST options have been stored for it.
		return nil, false
	}
	// Return a deep copy for safety, as message.Option values are byte slices.
	optsCopy := make([]message.Option, 0, len(resource.LastPutOrPostOptions))
	for _, o := range resource.LastPutOrPostOptions {
		valueCopy := make([]byte, len(o.Value))
		copy(valueCopy, o.Value)
		optsCopy = append(optsCopy, message.Option{ID: o.ID, Value: valueCopy})
	}
	return optsCopy, true
}

// MockCoAPClient provides a simplified CoAP client for use in integration tests,
// typically for interacting with a MockCoAPServer or another CoAP endpoint under test.
// It offers basic GET, POST, PUT, and DELETE operations using the `plgd-dev/go-coap` library.
//
// Note: For complex scenarios such as CoAP observation setup, detailed option manipulation,
// or handling of block-wise transfers, a more featured CoAP client library might be necessary,
// or this MockCoAPClient could be extended to support those specific features.
type MockCoAPClient struct {
	serverAddr string       // Target server address in "host:port" format.
	client     *client.Conn // Underlying CoAP client connection from `plgd-dev/go-coap/v3/udp/client`.
}

// NewMockCoAPClient creates a new MockCoAPClient instance targeting the specified server address.
// The `serverAddr` should be in "host:port" format (e.g., "127.0.0.1:5683").
func NewMockCoAPClient(serverAddr string) *MockCoAPClient {
	return &MockCoAPClient{
		serverAddr: serverAddr,
	}
}

// Connect establishes a UDP connection to the CoAP server specified by `serverAddr`.
// This method must be successfully called before any CoAP operations (GET, POST, PUT, DELETE)
// can be performed by the client. The provided `context.Context` can be used to set a timeout
// or enable cancellation for the dialing process. It uses `udp.Dial` from the `plgd-dev/go-coap` library.
func (c *MockCoAPClient) Connect(ctx context.Context) error {
	// `udp.Dial` is used for CoAP over UDP.
	// The context is passed via `options.WithContext` to control the dialing process (e.g., for timeout).
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
func (c *MockCoAPClient) Get(ctx context.Context, path string) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [GET %s] Client not connected; call Connect() first", path)
	}

	resp, err := c.client.Get(ctx, path) // client.Get handles token generation and message construction.
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [GET %s] Request failed: %w", path, err)
	}
	return resp.Message, nil // Return the underlying *message.Message
}

// Post performs a CoAP POST request to the specified path with the given content type and payload.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Post(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [POST %s] Client not connected; call Connect() first", path)
	}

	// The payload must be an io.Reader for the go-coap client.Post method.
	resp, err := c.client.Post(ctx, path, contentType, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [POST %s] Request failed: %w", path, err)
	}
	return resp.Message, nil
}

// Put performs a CoAP PUT request to the specified path with the given content type and payload.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Put(ctx context.Context, path string, contentType message.MediaType, payload []byte) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [PUT %s] Client not connected; call Connect() first", path)
	}

	// The payload must be an io.Reader for the go-coap client.Put method.
	resp, err := c.client.Put(ctx, path, contentType, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [PUT %s] Request failed: %w", path, err)
	}
	return resp.Message, nil
}

// Delete performs a CoAP DELETE request to the specified path.
// The provided context can be used for request timeouts or cancellation.
// Returns the server's response message or an error if the request fails.
func (c *MockCoAPClient) Delete(ctx context.Context, path string) (*message.Message, error) {
	if c.client == nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [DELETE %s] Client not connected; call Connect() first", path)
	}

	resp, err := c.client.Delete(ctx, path) // client.Delete handles message construction.
	if err != nil {
		return nil, fmt.Errorf("MockCoAPClient ERROR: [DELETE %s] Request failed: %w", path, err)
	}
	return resp.Message, nil
}
