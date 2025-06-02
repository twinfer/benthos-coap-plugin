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
