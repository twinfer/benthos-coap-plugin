package observer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp/client"
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// --- Mocks ---

// MockConnectionWrapper
type MockConnectionWrapper struct {
	mock.Mock
	conn      interface{}
	protocol  string
	endpoint  string
	closed    bool
	closeChan chan struct{}
}

func NewMockConnectionWrapper(conn interface{}, protocol, endpoint string) *MockConnectionWrapper {
	return &MockConnectionWrapper{
		conn:      conn,
		protocol:  protocol,
		endpoint:  endpoint,
		closeChan: make(chan struct{}),
	}
}
func (m *MockConnectionWrapper) Connection() interface{} { return m.conn }
func (m *MockConnectionWrapper) Protocol() string      { return m.protocol }
func (m *MockConnectionWrapper) Endpoint() string      { return m.endpoint }
func (m *MockConnectionWrapper) Close() error {
	if m.closed {
		return errors.New("already closed")
	}
	m.closed = true
	close(m.closeChan)
	return m.Called().Error(0)
}
func (m *MockConnectionWrapper) Closed() <-chan struct{} { return m.closeChan }
func (m *MockConnectionWrapper) Healthy() bool           { return !m.closed }


// MockConnectionManager
type MockConnectionManager struct {
	mock.Mock
}

func (m *MockConnectionManager) Get(ctx context.Context) (*connection.ConnectionWrapper, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*connection.ConnectionWrapper), args.Error(1)
}
func (m *MockConnectionManager) Put(conn *connection.ConnectionWrapper) { m.Called(conn) }
func (m *MockConnectionManager) Config() connection.Config {
	args := m.Called()
	return args.Get(0).(connection.Config)
}
func (m *MockConnectionManager) Close() error { return m.Called().Error(0) }

// MockConverter
type MockConverter struct {
	mock.Mock
}

func (m *MockConverter) CoAPToMessage(coapMsg *message.Message) (*service.Message, error) {
	args := m.Called(coapMsg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*service.Message), args.Error(1)
}
func (m *MockConverter) MessageToCoAP(msg *service.Message) (*message.Message, error) {
	args := m.Called(msg)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*message.Message), args.Error(1)
}

// MockUDPClientConn
type MockUDPClientConn struct {
	mock.Mock
	client.Conn // Embed to satisfy interface, but methods will be mocked
	observeFunc func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error)
}

func (m *MockUDPClientConn) Observe(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
	if m.observeFunc != nil {
		return m.observeFunc(ctx, path, observeFunc, opts...)
	}
	args := m.Called(ctx, path, observeFunc, opts)
	return args.Get(0).(message.Token), args.Error(1)
}
func (m *MockUDPClientConn) Close() error { return m.Called().Error(0) }
func (m *MockUDPClientConn) Ping(ctx context.Context) error { return m.Called(ctx).Error(0) }
func (m *MockUDPClientConn) RemoteAddr() coapNet.Addr { return nil } // Implement other necessary methods if they get called
func (m *MockUDPClientConn) Context() context.Context { return context.Background() }


// MockTCPClientConn
type MockTCPClientConn struct {
	mock.Mock
	tcpClient.Conn // Embed to satisfy interface
	observeFunc func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (*tcpClient.Observation, error)
}
func (m *MockTCPClientConn) Observe(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (*tcpClient.Observation, error) {
	if m.observeFunc != nil {
		return m.observeFunc(ctx, path, observeFunc, opts...)
	}
	args := m.Called(ctx, path, observeFunc, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tcpClient.Observation), args.Error(1)
}
func (m *MockTCPClientConn) Close() error { return m.Called().Error(0) }
func (m *MockTCPClientConn) Ping(ctx context.Context) error { return m.Called(ctx).Error(0) }
func (m *MockTCPClientConn) RemoteAddr() coapNet.Addr { return nil }
func (m *MockTCPClientConn) Context() context.Context { return context.Background() }


// MockTCPObservation
type MockTCPObservation struct {
	mock.Mock
	token message.Token
}
func (m *MockTCPObservation) Token() message.Token { return m.token }
func (m *MockTCPObservation) Cancel(ctx context.Context) error { return m.Called(ctx).Error(0) }
func (m *MockTCPObservation) Canceled() bool { args := m.Called(); return args.Bool(0) }


// --- Test Suite Setup ---
func newTestManager(t *testing.T, config Config) (*Manager, *MockConnectionManager, *MockConverter) {
	logger := service.MockLogger()
	mockConnMgr := new(MockConnectionManager)
	mockConv := new(MockConverter)

	// Default config for connManager if not specified by test
	connMgrConfig := connection.Config{
		Endpoints: []string{"coap://localhost:5683"},
		Protocol: "udp",
	}
	mockConnMgr.On("Config").Return(connMgrConfig).Maybe()


	mgr, err := NewManager(config, mockConnMgr, mockConv, logger, service.MockResources())
	require.NoError(t, err)
	require.NotNil(t, mgr)
	return mgr, mockConnMgr, mockConv
}

// --- Tests ---

func TestManager_HandleObserveMessage_Success(t *testing.T) {
	mgr, _, mockConv := newTestManager(t, Config{})
	defer mgr.Close()

	path := "/test/resource"
	token, _ := message.GetToken()
	coapMsg := pool.AcquireMessage(context.Background())
	defer pool.ReleaseMessage(coapMsg)
	coapMsg.SetCode(codes.Content)
	coapMsg.SetToken(token)
	coapMsg.SetBody(strings.NewReader("test payload"))
	coapMsg.SetObserve(10)

	benthosMsg := service.NewMessage([]byte("converted payload"))
	mockConv.On("CoAPToMessage", coapMsg).Return(benthosMsg, nil)

	// Need a subscription for handleObserveMessage to find
	mockUDPConn := new(MockUDPClientConn)
	mockUDPConn.On("Close").Return(nil).Maybe() // For when subscription is closed

	connWrapper := connection.NewConnectionWrapper(mockUDPConn, "udp", "localhost:5683", nil, service.MockLogger())

	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()

	sub := &Subscription{
		path:    path,
		conn:    connWrapper,
		circuit: NewCircuitBreaker(DefaultCircuitConfig()),
		cancel:  cancelSub,
		healthy: 1,
	}
	mgr.subscriptions[path] = sub


	mgr.handleObserveMessage(path, coapMsg)

	mockConv.AssertCalled(t, "CoAPToMessage", coapMsg)

	select {
	case outMsg := <-mgr.MessageChan():
		require.NotNil(t, outMsg)
		outPayload, err := outMsg.AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "converted payload", string(outPayload))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message on channel")
	}
}

func TestManager_HandleObserveMessage_ConverterError(t *testing.T) {
	mgr, _, mockConv := newTestManager(t, Config{})
	defer mgr.Close()

	path := "/test/resource"
	coapMsg := pool.AcquireMessage(context.Background())
	defer pool.ReleaseMessage(coapMsg)
	coapMsg.SetCode(codes.Content)

	mockConv.On("CoAPToMessage", coapMsg).Return(nil, errors.New("converter failed"))

	mockUDPConn := new(MockUDPClientConn)
	mockUDPConn.On("Close").Return(nil).Maybe()
	connWrapper := connection.NewConnectionWrapper(mockUDPConn, "udp", "localhost:5683", nil, service.MockLogger())
	subCtx, cancelSub := context.WithCancel(context.Background())
	defer cancelSub()
	sub := &Subscription{path: path, conn: connWrapper, circuit: NewCircuitBreaker(DefaultCircuitConfig()), cancel: cancelSub, healthy: 1}
	mgr.subscriptions[path] = sub

	mgr.handleObserveMessage(path, coapMsg)

	mockConv.AssertCalled(t, "CoAPToMessage", coapMsg)
	assert.Len(t, mgr.MessageChan(), 0, "Message channel should be empty after converter error")
}


func TestManager_PerformObserve_UDPSuccess(t *testing.T) {
	defaultConfig := Config{
		ObservePaths:   []string{"/test"},
		ObserveTimeout: 5 * time.Second,
		RetryPolicy:    RetryPolicy{MaxRetries: 1, InitialInterval: 10 * time.Millisecond},
		CircuitBreaker: DefaultCircuitConfig(),
	}
	mgr, mockConnMgr, mockConv := newTestManager(t, defaultConfig)
	defer mgr.Close() // This will cancel the manager's context

	mockUDP := new(MockUDPClientConn)
	mockConnWrapperActual := NewMockConnectionWrapper(mockUDP, "udp", "localhost:5683")

	// Use connection.ConnectionWrapper for the manager, but our MockConnectionWrapper for assertions/control
	connWrapperForMgr := &connection.ConnectionWrapper // The real one, wrapping our mock CoAP conn
	connWrapperForMgr = connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())


	subCtx, subCancel := context.WithCancel(mgr.ctx) // Derive from manager's context
	// defer subCancel() // subCancel will be called by performObserve's defer or by the test explicitly

	sub := &Subscription{
		path:    "/test",
		conn:    connWrapperForMgr,
		circuit: NewCircuitBreaker(defaultConfig.CircuitBreaker),
		cancel:  subCancel, // This cancel is for the subscription's own context passed to observeWithRetry
		healthy: 1,
	}

	token, _ := message.GetToken()
	notificationPayload := "udp notification"

	// Configure mock Observe for UDP
	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		assert.Equal(t, "/test", path)

		// Simulate receiving a notification
		go func() {
			time.Sleep(50 * time.Millisecond) // Short delay
			notif := pool.AcquireMessage(ctx) // Use the observe context
			defer pool.ReleaseMessage(notif)
			notif.SetCode(codes.Content)
			notif.SetToken(token)
			notif.SetBody(strings.NewReader(notificationPayload))
			notif.SetObserve(1)
			observeFunc(notif)

			// Simulate observation end by cancelling the context after a notification
			// In real CoAP, this might be due to server stopping observation or client cancelling
			// For this test, we can simulate it to make performObserve return.
			subCancel() // This will cause performObserve's <-observeCtx.Done() to unblock
		}()
		return token, nil
	}

	mockConv.On("CoAPToMessage", mock.AnythingOfTypeArgument("*message.Message")).Run(func(args mock.Arguments) {
		msg := args.Get(0).(*message.Message)
		payload, _ := msg.Body().Read(make([]byte, 100)) // Simplified read
		msg.Body().Seek(0,0) // Reset seeker
		assert.Contains(t, string(payload), notificationPayload)
	}).Return(service.NewMessage([]byte(notificationPayload)), nil)


	err := mgr.performObserve(subCtx, sub) // Pass the subscription's cancellable context
	require.NoError(t, err, "performObserve should succeed")

	// Check if message was received
	select {
	case msg := <-mgr.MessageChan():
		payload, _ := msg.AsBytes()
		assert.Equal(t, notificationPayload, string(payload))
	case <-time.After(200 * time.Millisecond):
		t.Error("Timeout waiting for notification message")
	}

	mockConv.AssertExpectations(t)
}


func TestManager_PerformObserve_TCPSuccess(t *testing.T) {
    defaultConfig := Config{
        ObservePaths:   []string{"/tcp_test"},
        ObserveTimeout: 5 * time.Second,
        RetryPolicy:    RetryPolicy{MaxRetries: 1, InitialInterval: 10 * time.Millisecond},
        CircuitBreaker: DefaultCircuitConfig(),
    }
    mgr, _, mockConv := newTestManager(t, defaultConfig)
    defer mgr.Close()

    mockTCP := new(MockTCPClientConn)
	mockTCPObs := new(MockTCPObservation)

    connWrapperForMgr := connection.NewConnectionWrapper(mockTCP, "tcp", "localhost:5684", func() {}, service.MockLogger())

    subCtx, subCancel := context.WithCancel(mgr.ctx)
    // defer subCancel() // subCancel is called by performObserve or explicitly

    sub := &Subscription{
        path:    "/tcp_test",
        conn:    connWrapperForMgr,
        circuit: NewCircuitBreaker(defaultConfig.CircuitBreaker),
        cancel:  subCancel,
        healthy: 1,
    }

    token, _ := message.GetToken()
	mockTCPObs.token = token
    notificationPayload := "tcp notification"

    // Configure mock Observe for TCP
	mockTCP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (*tcpClient.Observation, error) {
        assert.Equal(t, "/tcp_test", path)

        // Simulate receiving a notification
        go func() {
            time.Sleep(50 * time.Millisecond)
            notif := pool.AcquireMessage(ctx)
            defer pool.ReleaseMessage(notif)
            notif.SetCode(codes.Content)
            notif.SetToken(token)
            notif.SetBody(strings.NewReader(notificationPayload))
            notif.SetObserve(1)
            observeFunc(notif)

			// Simulate observation ending
			subCancel()
        }()
        return mockTCPObs, nil
    }
	mockTCPObs.On("Token").Return(token)
	mockTCPObs.On("Cancel", mock.Anything).Return(nil).Once() // Expect Cancel to be called

    mockConv.On("CoAPToMessage", mock.AnythingOfTypeArgument("*message.Message")).Return(service.NewMessage([]byte(notificationPayload)), nil)

    err := mgr.performObserve(subCtx, sub)
    require.NoError(t, err, "performObserve for TCP should succeed")

    select {
    case msg := <-mgr.MessageChan():
        payload, _ := msg.AsBytes()
        assert.Equal(t, notificationPayload, string(payload))
    case <-time.After(200 * time.Millisecond):
        t.Error("Timeout waiting for TCP notification message")
    }

    mockConv.AssertExpectations(t)
	mockTCPObs.AssertCalled(t, "Cancel", mock.Anything)
}


func TestManager_PerformObserve_ObserveErrorUDP(t *testing.T) {
	defaultConfig := Config{ObserveTimeout: 100 * time.Millisecond}
	mgr, _, _ := newTestManager(t, defaultConfig)
	defer mgr.Close()

	mockUDP := new(MockUDPClientConn)
	connWrapperForMgr := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())

	subCtx, subCancel := context.WithCancel(mgr.ctx)
	defer subCancel()
	sub := &Subscription{path: "/test_err", conn: connWrapperForMgr, circuit: NewCircuitBreaker(DefaultCircuitConfig()), cancel: subCancel}

	expectedErr := errors.New("udp observe failed")
	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		return nil, expectedErr
	}

	err := mgr.performObserve(subCtx, sub)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestManager_PerformObserve_ContextCancelled(t *testing.T) {
	defaultConfig := Config{ObserveTimeout: 5 * time.Second} // Long timeout
	mgr, _, _ := newTestManager(t, defaultConfig)
	// Do not defer mgr.Close() here, as we want to control sub-context cancellation manually for the test

	mockUDP := new(MockUDPClientConn)
	connWrapperForMgr := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())

	managerCtx, managerCancel := context.WithCancel(context.Background()) // Independent manager ctx for this test
	mgr.ctx = managerCtx // Override manager's context

	subCtx, subCancel := context.WithCancel(managerCtx) // Sub-context for performObserve

	sub := &Subscription{path: "/test_cancel", conn: connWrapperForMgr, circuit: NewCircuitBreaker(DefaultCircuitConfig()), cancel: subCancel}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)

	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		// Simulate blocking until context is cancelled
		go func() {
			defer waitGroup.Done()
			<-ctx.Done() // Wait for cancellation
		}()
		return message.Token("token"), nil // Successfully started observation
	}

	// Cancel the subCtx *before* calling performObserve to simulate immediate cancellation
	subCancel()

	err := mgr.performObserve(subCtx, sub)
	require.Error(t, err, "Error should be context.Canceled or DeadlineExceeded if timeout was also very short")
	assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "Expected context.Canceled or DeadlineExceeded")

	waitGroup.Wait() // Ensure the observeFunc goroutine exits
	managerCancel() // Clean up manager context
}

func TestManager_ObserveWithRetry_RetriesAndSucceeds(t *testing.T) {
	retryAttempts := 0
	maxRetries := 2 // Manager will try original + 2 retries = 3 attempts
	expectedObserveCalls := maxRetries + 1

	config := Config{
		ObservePaths:     []string{"/test"},
		RetryPolicy:      RetryPolicy{MaxRetries: maxRetries, InitialInterval: 5 * time.Millisecond, MaxInterval: 20 * time.Millisecond, Multiplier: 1.5},
		CircuitBreaker:   DefaultCircuitConfig(), // Keep CB simple for this test
		BufferSize:       10,
		ObserveTimeout:   100 * time.Millisecond,
		ResubscribeDelay: 50 * time.Millisecond,
	}
	mgr, mockConnMgr, mockConv := newTestManager(t, config)
	// No defer mgr.Close() yet, we want to control lifecycle

	mockUDP := new(MockUDPClientConn)
	mockConnWrapperForMgr := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockConnWrapperForMgr, nil)
	mockConnMgr.On("Put", mockConnWrapperForMgr).Return()
	mockConnMgr.On("Close").Return(nil).Maybe() // For mgr.Close()

	notificationPayload := "udp notification after retries"
	observeCallCount := 0

	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		observeCallCount++
		if observeCallCount <= maxRetries { // Fail for initial attempts
			retryAttempts++
			return nil, errors.New(fmt.Sprintf("simulated observe error attempt %d", observeCallCount))
		}
		// Succeed on the last attempt
		token, _ := message.GetToken()
		go func() { // Simulate async notification
			time.Sleep(10 * time.Millisecond)
			notif := pool.AcquireMessage(ctx)
			defer pool.ReleaseMessage(notif)
			notif.SetCode(codes.Content)
			notif.SetToken(token)
			notif.SetBody(strings.NewReader(notificationPayload))
			notif.SetObserve(1)
			observeFunc(notif)
			// No explicit cancel here, let performObserve timeout or main test context cancel
		}()
		return token, nil
	}

	mockConv.On("CoAPToMessage", mock.AnythingOfTypeArgument("*message.Message")).Return(service.NewMessage([]byte(notificationPayload)), nil).Maybe()

	// Start the manager's subscription process
	err := mgr.Subscribe("/test")
	require.NoError(t, err)

	// Wait for the message to arrive or timeout
	select {
	case msg := <-mgr.MessageChan():
		payload, _ := msg.AsBytes()
		assert.Equal(t, notificationPayload, string(payload))
	case <-time.After(500 * time.Millisecond): // Increased timeout for retries
		t.Fatal("Timeout waiting for message after retries")
	}

	mgr.Close() // Close manager to stop observation goroutines

	assert.Equal(t, maxRetries, retryAttempts, "Should have retried specified number of times")
	assert.Equal(t, expectedObserveCalls, observeCallCount, "Observe method should be called expected number of times")

	// Check subscription health (should be healthy after success)
	sub := mgr.getSubscription("/test")
	require.NotNil(t, sub)
	assert.True(t, sub.circuit.IsHealthy(), "Circuit breaker should be healthy")
	assert.Equal(t, int32(0), sub.retryCount, "Subscription retry count should be reset after success")
}


func TestManager_ObserveWithRetry_MaxRetriesExceeded(t *testing.T) {
	maxRetries := 2
	expectedObserveCalls := maxRetries + 1 // Initial attempt + maxRetries

	config := Config{
		ObservePaths:     []string{"/test_max_retry"},
		RetryPolicy:      RetryPolicy{MaxRetries: maxRetries, InitialInterval: 5 * time.Millisecond, MaxInterval: 10 * time.Millisecond, Multiplier: 1.2},
		CircuitBreaker:   DefaultCircuitConfig(),
		BufferSize:       10,
		ObserveTimeout:   50 * time.Millisecond,
	}
	mgr, mockConnMgr, _ := newTestManager(t, config)
	// No defer mgr.Close() yet

	mockUDP := new(MockUDPClientConn)
	mockConnWrapperForMgr := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockConnWrapperForMgr, nil)
	mockConnMgr.On("Put", mockConnWrapperForMgr).Return()
	mockConnMgr.On("Close").Return(nil).Maybe()


	observeCallCount := 0
	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		observeCallCount++
		return nil, errors.New(fmt.Sprintf("persistent observe error %d", observeCallCount))
	}

	err := mgr.Subscribe("/test_max_retry")
	require.NoError(t, err)

	// Wait long enough for all retries to exhaust
	time.Sleep(100 * time.Millisecond)

	mgr.Close()

	assert.Equal(t, expectedObserveCalls, observeCallCount, "Observe method should be called for initial + all retries")

	sub := mgr.getSubscription("/test_max_retry")
	require.NotNil(t, sub)
	assert.False(t, sub.circuit.IsHealthy(), "Circuit breaker should be unhealthy (or at least not explicitly healthy)")
	assert.Equal(t, int32(expectedObserveCalls), sub.retryCount, "Subscription retry count should reflect all attempts")
	assert.Equal(t, int32(0), sub.healthy, "Subscription should be marked as unhealthy")
}

func TestManager_ObserveWithRetry_CircuitBreaker(t *testing.T) {
	cbFailures := 2
	cbResetInterval := 50 * time.Millisecond
	config := Config{
		ObservePaths:   []string{"/test_cb"},
		RetryPolicy:    RetryPolicy{MaxRetries: 5, InitialInterval: 5 * time.Millisecond}, // Allow enough retries
		CircuitBreaker: CircuitConfig{Enabled: true, FailureThreshold: uint32(cbFailures), ResetInterval: cbResetInterval, OpenStateDelay: cbResetInterval},
		BufferSize:     10,
		ObserveTimeout: 30 * time.Millisecond,
	}
	mgr, mockConnMgr, mockConv := newTestManager(t, config)

	mockUDP := new(MockUDPClientConn)
	mockConnWrapperForMgr := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func() {}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockConnWrapperForMgr, nil)
	mockConnMgr.On("Put", mockConnWrapperForMgr).Return()
	mockConnMgr.On("Close").Return(nil).Maybe()

	observeCallCount := 0
	notificationPayload := "cb notification"
	var failObserve bool = true // Start by failing

	mockUDP.observeFunc = func(ctx context.Context, path string, observeFunc func(notification *message.Message), opts ...options.Option) (message.Token, error) {
		observeCallCount++
		if failObserve {
			return nil, errors.New(fmt.Sprintf("cb observe error %d", observeCallCount))
		}
		// Succeed after circuit breaker has reset
		token, _ := message.GetToken()
		go func() {
			time.Sleep(5 * time.Millisecond)
			notif := pool.AcquireMessage(ctx); defer pool.ReleaseMessage(notif)
			notif.SetCode(codes.Content); notif.SetToken(token); notif.SetBody(strings.NewReader(notificationPayload)); notif.SetObserve(1)
			observeFunc(notif)
		}()
		return token, nil
	}
	mockConv.On("CoAPToMessage", mock.AnythingOfTypeArgument("*message.Message")).Return(service.NewMessage([]byte(notificationPayload)), nil).Maybe()


	err := mgr.Subscribe("/test_cb")
	require.NoError(t, err)

	// Wait for initial failures to open the circuit
	time.Sleep(time.Duration(cbFailures+1) * (config.RetryPolicy.InitialInterval + config.ObserveTimeout) + 10*time.Millisecond) // Ensure enough time for failures

	sub := mgr.getSubscription("/test_cb")
	require.NotNil(t, sub)
	assert.Equal(t, CircuitStateOpen, sub.circuit.State(), "Circuit should be open after enough failures")
	callsWhileOpen := observeCallCount // Store calls before circuit reset

	// Wait for circuit breaker to reset and potentially one retry attempt in half-open
	time.Sleep(cbResetInterval + config.RetryPolicy.InitialInterval + 10*time.Millisecond)

	failObserve = false // Now allow observe to succeed

	// Wait for successful notification or timeout
	select {
	case <-mgr.MessageChan():
		// Message received
	case <-time.After(cbResetInterval + config.ObserveTimeout + 50*time.Millisecond): // Wait for recovery attempt
		t.Fatal("Timeout waiting for message after circuit breaker reset")
	}

	mgr.Close()

	assert.True(t, observeCallCount > callsWhileOpen, "Observe should have been called again after circuit reset")
	assert.True(t, sub.circuit.IsHealthy(), "Circuit breaker should be healthy after success")
}
