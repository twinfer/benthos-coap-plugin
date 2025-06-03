package output

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/options"
	coapNet "github.com/plgd-dev/go-coap/v3/net"
	udpClient "github.com/plgd-dev/go-coap/v3/udp/client"
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
	"github.com/twinfer/benthos-coap-plugin/pkg/converter"
)

// --- Mocks (can be shared or adapted from observer tests) ---

// MockConnectionWrapper (assuming it's defined as in observer_test or in a shared test util)
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
	// Ensure we return an error if one is expected by the mock setup for Close()
	// If no specific error is set for Close(), return nil by default for success.
	args := m.Called()
	return args.Error(0)
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
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*service.Message), args.Error(1)
}
func (m *MockConverter) MessageToCoAP(msg *service.Message) (*message.Message, error) {
	args := m.Called(msg)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*message.Message), args.Error(1)
}


func TestWriteWithRetry_ConnManagerGetError(t *testing.T) {
	outputConf := OutputConfig{DefaultPath: "/default"}
	out, mockConnMgr, _ := newTestOutput(t, outputConf)

	expectedErr := errors.New("conn manager Get failed")
	mockConnMgr.On("Get", mock.Anything).Return(nil, expectedErr)

	benthosMsg := service.NewMessage([]byte("test"))
	err := out.Write(context.Background(), benthosMsg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockConnMgr.AssertCalled(t, "Get", mock.Anything)
}

func TestWriteWithRetry_ConverterMessageToCoAPError(t *testing.T) {
	outputConf := OutputConfig{DefaultPath: "/default"}
	out, mockConnMgr, mockConv := newTestOutput(t, outputConf)

	mockUDP := new(MockUDPClientConn) // Real conn not strictly needed as converter fails first
	mockReturnedConnWrapper := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func(){}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockReturnedConnWrapper, nil)
	mockConnMgr.On("Put", mockReturnedConnWrapper).Return() // Expect Put because Get succeeded

	expectedErr := errors.New("converter MessageToCoAP failed")
	benthosMsg := service.NewMessage([]byte("test"))
	mockConv.On("MessageToCoAP", benthosMsg).Return(nil, expectedErr)

	err := out.Write(context.Background(), benthosMsg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	mockConnMgr.AssertCalled(t, "Get", mock.Anything)
	mockConnMgr.AssertCalled(t, "Put", mockReturnedConnWrapper)
	mockConv.AssertCalled(t, "MessageToCoAP", benthosMsg)
}


// MockUDPClientConn
type MockUDPClientConn struct {
	mock.Mock
	udpClient.Conn // Embed for interface fulfillment
	doFunc func(ctx context.Context, req *message.Message) (*message.Message, error)
}
func (m *MockUDPClientConn) Do(ctx context.Context, req *message.Message) (*message.Message, error) {
	if m.doFunc != nil { return m.doFunc(ctx, req) }
	args := m.Called(ctx, req)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*message.Message), args.Error(1)
}
func (m *MockUDPClientConn) Close() error { return m.Called().Error(0) }
func (m *MockUDPClientConn) Ping(ctx context.Context) error { return m.Called(ctx).Error(0) }
func (m *MockUDPClientConn) RemoteAddr() coapNet.Addr { return nil }
func (m *MockUDPClientConn) Context() context.Context { return context.Background() }


// MockTCPClientConn
type MockTCPClientConn struct {
	mock.Mock
	tcpClient.Conn // Embed for interface fulfillment
	doFunc func(ctx context.Context, req *message.Message) (*message.Message, error)
}
func (m *MockTCPClientConn) Do(ctx context.Context, req *message.Message) (*message.Message, error) {
	if m.doFunc != nil { return m.doFunc(ctx, req) }
	args := m.Called(ctx, req)
	if args.Get(0) == nil { return nil, args.Error(1) }
	return args.Get(0).(*message.Message), args.Error(1)
}
func (m *MockTCPClientConn) Close() error { return m.Called().Error(0) }
func (m *MockTCPClientConn) Ping(ctx context.Context) error { return m.Called(ctx).Error(0) }
func (m *MockTCPClientConn) RemoteAddr() coapNet.Addr { return nil }
func (m *MockTCPClientConn) Context() context.Context { return context.Background() }


// --- Test Suite Setup ---
func newTestOutput(t *testing.T, outputConf OutputConfig) (*Output, *MockConnectionManager, *MockConverter) {
	logger := service.MockLogger()
	mockConnMgr := new(MockConnectionManager)
	mockConv := new(MockConverter)

	// Provide default connection manager config
	connMgrConf := connection.Config{
		Endpoints: outputConf.Endpoints,
		Protocol:  outputConf.Protocol,
	}
	mockConnMgr.On("Config").Return(connMgrConf).Maybe()

	// Create the output using a helper that mirrors `newCoAPOutput` but with mocks
	out := &Output{
		connManager: mockConnMgr,
		converter:   mockConv,
		config:      outputConf,
		logger:      logger,
		metrics: &Metrics{ // Initialize metrics to avoid nil pointer dereferences
			MessagesSent:    service.MockMetricCounter(),
			MessagesFailed:  service.MockMetricCounter(),
			RequestsTotal:   service.MockMetricCounter(),
			RequestsSuccess: service.MockMetricCounter(),
			RequestsTimeout: service.MockMetricCounter(),
			RetriesTotal:    service.MockMetricCounter(),
			ConnectionsUsed: service.MockMetricCounter(),
		},
	}
	return out, mockConnMgr, mockConv
}


// --- Tests ---

func TestSendCoAPMessage_Delegation(t *testing.T) {
	defaultOutputConf := OutputConfig{
		Protocol: "udp", // Default, will be overridden by mock conn wrapper
		RequestOptions: RequestOptions{Timeout: 5 * time.Second},
	}

	tests := []struct {
		name         string
		protocol     string
		connClient   interface{} // *MockUDPClientConn or *MockTCPClientConn
		expectUDPCall bool
		expectTCPCall bool
		expectedError string
	}{
		{
			name: "UDP protocol", protocol: "udp", connClient: new(MockUDPClientConn),
			expectUDPCall: true, expectTCPCall: false,
		},
		{
			name: "DTLS protocol (uses UDP conn)", protocol: "udp-dtls", connClient: new(MockUDPClientConn),
			expectUDPCall: true, expectTCPCall: false,
		},
		{
			name: "TCP protocol", protocol: "tcp", connClient: new(MockTCPClientConn),
			expectUDPCall: false, expectTCPCall: true,
		},
		{
			name: "TLS protocol (uses TCP conn)", protocol: "tcp-tls", connClient: new(MockTCPClientConn),
			expectUDPCall: false, expectTCPCall: true,
		},
		{
			name: "Unsupported protocol in wrapper", protocol: "http", connClient: new(MockUDPClientConn), // connClient type won't matter here
			expectedError: "unsupported connection type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, _, mockConv := newTestOutput(t, defaultOutputConf)

			mockConnWrapper := NewMockConnectionWrapper(tt.connClient, tt.protocol, "localhost:1234")

			coapMsg := pool.AcquireMessage(context.Background())
			defer pool.ReleaseMessage(coapMsg)
			coapMsg.SetCode(codes.POST)
			coapMsg.SetToken(message.Token("test"))
			// mockConv.On("MessageToCoAP", mock.Anything).Return(coapMsg, nil) // Not needed for sendCoAPMessage directly

			if tt.expectUDPCall {
				mockUDP := tt.connClient.(*MockUDPClientConn)
				// Mock the Do func to return success immediately
				mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					resp := pool.AcquireMessage(ctx)
					resp.SetCode(codes.Changed)
					return resp, nil
				}
			}
			if tt.expectTCPCall {
				mockTCP := tt.connClient.(*MockTCPClientConn)
				mockTCP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					resp := pool.AcquireMessage(ctx)
					resp.SetCode(codes.Changed)
					return resp, nil
				}
			}

			err := out.sendCoAPMessage(context.Background(), (*connection.ConnectionWrapper)(mockConnWrapper), coapMsg, "/testpath")

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}


func TestOutput_Close(t *testing.T) {
	out, mockConnMgr, _ := newTestOutput(t, OutputConfig{})

	mockConnMgr.On("Close").Return(nil).Once()

	err := out.Close(context.Background())
	assert.NoError(t, err)
	mockConnMgr.AssertCalled(t, "Close")

	// Test close error
	expectedErr := errors.New("conn manager close failed")
	mockConnMgrError := new(MockConnectionManager) // New mock for error case
	mockConnMgrError.On("Config").Return(connection.Config{})
	mockConnMgrError.On("Close").Return(expectedErr).Once()

	outError := &Output{ // Create new output with erroring mock
		connManager: mockConnMgrError,
		logger:      service.MockLogger(),
		metrics:     &Metrics{},
	}
	err = outError.Close(context.Background())
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockConnMgrError.AssertCalled(t, "Close")
}

func TestSendTCPMessage_ErrorCases(t *testing.T) {
	out, _, _ := newTestOutput(t, OutputConfig{RequestOptions: RequestOptions{Timeout: 20 * time.Millisecond}}) // Short timeout
	mockTCP := new(MockTCPClientConn)
	reqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(reqMsg)
	reqMsg.SetCode(codes.POST)

	tests := []struct{
		name string
		setupMock func()
		expectedErrorContains string
	}{
		{
			name: "Do returns error",
			setupMock: func() {
				mockTCP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					return nil, errors.New("tcp do failed")
				}
			},
			expectedErrorContains: "tcp do failed",
		},
		{
			name: "Non-2xx response code",
			setupMock: func() {
				mockTCP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					resp := pool.AcquireMessage(ctx)
					resp.SetCode(codes.BadGateway)
					return resp, nil
				}
			},
			expectedErrorContains: "returned error code 5.02",
		},
		{
			name: "Request timeout",
			setupMock: func() {
				mockTCP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					time.Sleep(50 * time.Millisecond) // Longer than timeout
					return nil, errors.New("should have timed out")
				}
			},
			expectedErrorContains: "timed out",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){
			tt.setupMock()
			err := out.sendTCPMessage(context.Background(), mockTCP, reqMsg, "ep", "/p", "tok")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErrorContains)
		})
	}
}


func TestSendUDPMessage_Success(t *testing.T) {
	out, _, _ := newTestOutput(t, OutputConfig{RequestOptions: RequestOptions{Timeout: 100 * time.Millisecond}})
	mockUDP := new(MockUDPClientConn)

	reqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(reqMsg)
	reqMsg.SetCode(codes.POST) // This code is on the message from converter, send*Message will use POST by default for the actual request.
	reqMsg.SetToken(message.Token("token123"))
	reqMsg.SetPathString("/testpath") // Path set by converter
	reqMsg.SetPayload([]byte("test payload"))
	reqMsg.SetType(message.Confirmable)
	err := reqMsg.SetOptionUint32(message.ContentFormat, message.TextPlain) // Option from converter
	require.NoError(t, err)


	mockUDP.doFunc = func(ctx context.Context, actualReq *message.Message) (*message.Message, error) {
		// Verify the message sent by sendUDPMessage via conn.Do()
		assert.Equal(t, codes.POST, actualReq.Code(), "Request code should be POST (default in sendUDPMessage)")
		assert.Equal(t, reqMsg.Token(), actualReq.Token(), "Token should match")

		path, pErr := actualReq.Path()
		require.NoError(t, pErr)
		assert.Equal(t, "testpath", path, "Path should match")

		assert.Equal(t, reqMsg.Payload(), actualReq.Payload(), "Payload should match")
		assert.Equal(t, reqMsg.Type(), actualReq.Type(), "Message type should match")

		cf, cfErr := actualReq.Options().GetUint32(message.ContentFormat)
		require.NoError(t, cfErr, "ContentFormat option should be present")
		assert.Equal(t, uint32(message.TextPlain), cf, "ContentFormat should match")

		resp := pool.AcquireMessage(ctx)
		resp.SetCode(codes.Created)
		return resp, nil
	}

	err := out.sendUDPMessage(context.Background(), mockUDP, reqMsg, "endpoint", "/test", "token")
	require.NoError(t, err)
}

func TestSendTCPMessage_Success(t *testing.T) {
	out, _, _ := newTestOutput(t, OutputConfig{RequestOptions: RequestOptions{Timeout: 100 * time.Millisecond}})
	mockTCP := new(MockTCPClientConn)

	reqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(reqMsg)
	reqMsg.SetCode(codes.PUT) // This code is on the message from converter. sendTCPMessage will use POST.
	reqMsg.SetToken(message.Token("tokenTCP123"))
	reqMsg.SetPathString("/tcp_resource_path")
	reqMsg.SetPayload([]byte("tcp test payload"))
	reqMsg.SetType(message.NonConfirmable)
	err := reqMsg.SetOptionUint32(message.ContentFormat, message.AppJSON)
	require.NoError(t, err)


	mockTCP.doFunc = func(ctx context.Context, actualReq *message.Message) (*message.Message, error) {
		assert.Equal(t, codes.POST, actualReq.Code(), "Request code should be POST (default in sendTCPMessage)")
		assert.Equal(t, reqMsg.Token(), actualReq.Token(), "Token should match")

		path, pErr := actualReq.Path()
		require.NoError(t, pErr)
		assert.Equal(t, "tcp_resource_path", path, "Path should match")

		assert.Equal(t, reqMsg.Payload(), actualReq.Payload(), "Payload should match")
		assert.Equal(t, reqMsg.Type(), actualReq.Type(), "Message type should match")

		cf, cfErr := actualReq.Options().GetUint32(message.ContentFormat)
		require.NoError(t, cfErr, "ContentFormat option should be present")
		assert.Equal(t, uint32(message.AppJSON), cf, "ContentFormat should match")

		resp := pool.AcquireMessage(ctx)
		resp.SetCode(codes.Changed)
		return resp, nil
	}

	err := out.sendTCPMessage(context.Background(), mockTCP, reqMsg, "endpoint", "/tcp_resource", "tokenTCP")
	require.NoError(t, err)
}


func TestSendUDPMessage_ErrorCases(t *testing.T) {
	out, _, _ := newTestOutput(t, OutputConfig{RequestOptions: RequestOptions{Timeout: 20 * time.Millisecond}}) // Short timeout
	mockUDP := new(MockUDPClientConn)
	reqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(reqMsg)
	reqMsg.SetCode(codes.POST)

	tests := []struct{
		name string
		setupMock func()
		expectedErrorContains string
	}{
		{
			name: "Do returns error",
			setupMock: func() {
				mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					return nil, errors.New("udp do failed")
				}
			},
			expectedErrorContains: "udp do failed",
		},
		{
			name: "Non-2xx response code",
			setupMock: func() {
				mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					resp := pool.AcquireMessage(ctx)
					resp.SetCode(codes.NotFound)
					return resp, nil
				}
			},
			expectedErrorContains: "returned error code 4.04",
		},
		{
			name: "Request timeout",
			setupMock: func() {
				mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
					time.Sleep(50 * time.Millisecond) // Longer than timeout
					return nil, errors.New("should have timed out") // This error shouldn't be returned if timeout works
				}
			},
			expectedErrorContains: "timed out", // context.DeadlineExceeded.Error() is "context deadline exceeded"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){
			tt.setupMock()
			err := out.sendUDPMessage(context.Background(), mockUDP, reqMsg, "ep", "/p", "tok")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErrorContains)
		})
	}
}


func TestWriteWithRetry_RetriesAndSucceeds(t *testing.T) {
	maxRetries := 2
	outputConf := OutputConfig{
		DefaultPath: "/default",
		RetryPolicy: RetryPolicy{MaxRetries: maxRetries, InitialInterval: 5 * time.Millisecond, MaxInterval: 10 * time.Millisecond},
		RequestOptions: RequestOptions{Timeout: 50*time.Millisecond, Confirmable: true},
	}
	out, mockConnMgr, mockConv := newTestOutput(t, outputConf)

	mockUDP := new(MockUDPClientConn)
	// Corrected: Use the actual connection.ConnectionWrapper for the manager to return
	mockReturnedConnWrapper := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func(){}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockReturnedConnWrapper, nil)
	mockConnMgr.On("Put", mockReturnedConnWrapper).Return() // Expect Put to be called with the same wrapper
	mockConnMgr.On("Close").Return(nil).Maybe()


	benthosMsg := service.NewMessage([]byte("test content"))
	coapReqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(coapReqMsg)
	coapReqMsg.SetCode(codes.POST) // Default method
	coapReqMsg.SetType(message.Confirmable) // From RequestOptions
	coapReqMsg.SetPathString("/default")    // From DefaultPath
	coapReqMsg.SetPayload([]byte("test content"))

	mockConv.On("MessageToCoAP", benthosMsg).Return(coapReqMsg, nil)

	sendAttempt := 0
	mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
		sendAttempt++
		if sendAttempt <= maxRetries {
			return nil, errors.New("simulated send error")
		}
		resp := pool.AcquireMessage(ctx)
		resp.SetCode(codes.Changed)
		return resp, nil
	}

	err := out.Write(context.Background(), benthosMsg)
	require.NoError(t, err)
	assert.Equal(t, maxRetries + 1, sendAttempt, "Should succeed after retries")

	mockConnMgr.AssertCalled(t, "Get", mock.Anything)
	mockConnMgr.AssertCalled(t, "Put", mockReturnedConnWrapper)
	mockConv.AssertCalled(t, "MessageToCoAP", benthosMsg)
}

// TODO: Add more tests:
// - TestWriteWithRetry_MaxRetriesExceeded
// - TestWriteWithRetry_ContextCancelledDuringBackoff
// - Test correct CoAP message construction in sendUDP/TCPMessage (options, token, type from converter)
// - Test that path from metadata is used if present
// - Test error from connManager.Get()
// - Test error from converter.MessageToCoAP()
// - Test Close() method of Output
// - Test Health() method
// - Test parsing of configs (if complex logic exists there)


func TestWriteWithRetry_MaxRetriesExceeded(t *testing.T) {
	maxRetries := 1 // Keep low for faster test
	outputConf := OutputConfig{
		DefaultPath: "/default",
		RetryPolicy: RetryPolicy{MaxRetries: maxRetries, InitialInterval: 5 * time.Millisecond},
		RequestOptions: RequestOptions{Timeout: 20*time.Millisecond, Confirmable: true},
	}
	out, mockConnMgr, mockConv := newTestOutput(t, outputConf)

	mockUDP := new(MockUDPClientConn)
	mockReturnedConnWrapper := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func(){}, service.MockLogger())

	// Setup mock calls
	mockConnMgr.On("Get", mock.Anything).Return(mockReturnedConnWrapper, nil)
	// Expect Put to be called once for each attempt (initial + retries) that involves a conn.Get
	// Since sendCoAPMessage is called initial + maxRetries times.
	mockConnMgr.On("Put", mockReturnedConnWrapper).Times(maxRetries + 1)


	benthosMsg := service.NewMessage([]byte("test content"))
	coapReqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(coapReqMsg)
	// Populate coapReqMsg as MessageToCoAP would
	coapReqMsg.SetCode(codes.POST)
	coapReqMsg.SetType(message.Confirmable)
	coapReqMsg.SetPathString("/default")
	coapReqMsg.SetPayload([]byte("test content"))

	mockConv.On("MessageToCoAP", benthosMsg).Return(coapReqMsg, nil)

	sendAttempt := 0
	expectedErrorFromDo := "persistent send error"
	mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
		sendAttempt++
		return nil, errors.New(expectedErrorFromDo) // Always fail
	}

	err := out.Write(context.Background(), benthosMsg)
	require.Error(t, err, "Write should fail after max retries")
	assert.Contains(t, err.Error(), expectedErrorFromDo, "Error should contain the underlying send error")
	assert.Contains(t, err.Error(), fmt.Sprintf("after %d retries", maxRetries), "Error message should indicate max retries hit")
	assert.Equal(t, maxRetries + 1, sendAttempt, "Should have attempted send maxRetries + 1 times")

	mockConnMgr.AssertExpectations(t)
	mockConv.AssertCalled(t, "MessageToCoAP", benthosMsg)
}


func TestWriteWithRetry_ContextCancelledDuringBackoff(t *testing.T) {
	outputConf := OutputConfig{
		DefaultPath: "/default",
		RetryPolicy: RetryPolicy{MaxRetries: 1, InitialInterval: 100 * time.Millisecond}, // Long enough interval
		RequestOptions: RequestOptions{Timeout: 20*time.Millisecond},
	}
	out, mockConnMgr, mockConv := newTestOutput(t, outputConf)
	parentCtx, cancelParent := context.WithCancel(context.Background())

	mockUDP := new(MockUDPClientConn)
	mockReturnedConnWrapper := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func(){}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockReturnedConnWrapper, nil)
	mockConnMgr.On("Put", mockReturnedConnWrapper).Return() // Expect Put for the first attempt

	benthosMsg := service.NewMessage([]byte("test content"))
	coapReqMsg := pool.AcquireMessage(context.Background()); defer pool.ReleaseMessage(coapReqMsg)
	mockConv.On("MessageToCoAP", benthosMsg).Return(coapReqMsg, nil)

	// First attempt fails, triggering retry and backoff
	mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
		// Cancel the parent context shortly after the first failure, during the backoff period
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancelParent()
		}()
		return nil, errors.New("initial send error")
	}

	err := out.Write(parentCtx, benthosMsg) // Use the cancellable parentCtx
	require.Error(t, err, "Write should fail due to context cancellation")
	assert.True(t, errors.Is(err, context.Canceled) || strings.Contains(err.Error(), context.Canceled.Error()), "Error should be context.Canceled or wrap it")

	mockConnMgr.AssertCalled(t, "Get", mock.Anything)
	mockConnMgr.AssertCalled(t, "Put", mockReturnedConnWrapper) // Only once for the first attempt
	mockConv.AssertCalled(t, "MessageToCoAP", benthosMsg)
}


// Placeholder for a test that checks path handling
func TestWriteWithRetry_PathHandling(t *testing.T) {
	outputConf := OutputConfig{
		DefaultPath: "/default",
		RequestOptions: RequestOptions{Timeout: 50*time.Millisecond, Confirmable: true},
	}
	out, mockConnMgr, mockConv := newTestOutput(t, outputConf)

	mockUDP := new(MockUDPClientConn)
	mockReturnedConnWrapper := connection.NewConnectionWrapper(mockUDP, "udp", "localhost:5683", func(){}, service.MockLogger())

	mockConnMgr.On("Get", mock.Anything).Return(mockReturnedConnWrapper, nil)
	mockConnMgr.On("Put", mockReturnedConnWrapper).Return()

	tests := []struct{
		name string
		msgMeta map[string]string
		expectedPath string
	}{
		{"default path", map[string]string{}, "/default"},
		{"metadata path", map[string]string{"coap_path": "/meta/path"}, "/meta/path"},
		{"empty metadata path uses default", map[string]string{"coap_path": ""}, "/default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T){
			benthosMsg := service.NewMessage([]byte("p"))
			for k,v := range tt.msgMeta {
				benthosMsg.MetaSet(k,v)
			}

			coapReqMsg := pool.AcquireMessage(context.Background())
			coapReqMsg.SetCode(codes.POST)
			coapReqMsg.SetType(message.Confirmable)
			// Path will be set by MessageToCoAP or by WriteWithRetry's logic

			mockConv.On("MessageToCoAP", benthosMsg).Run(func(args mock.Arguments){
				// This is a bit of a hack to inject the expected path into the converted message
				// In a real scenario, converter.MessageToCoAP would use the metadata to set options including path.
				// For this test, we ensure the coapMsg passed to sendCoAPMessage has the right path.
				// The WriteWithRetry logic sets path based on metadata BEFORE MessageToCoAP is called.
				// MessageToCoAP should then use that path.
				// This test actually tests WriteWithRetry's path logic more than MessageToCoAP's.
			}).Return(coapReqMsg, nil).Once()


			mockUDP.doFunc = func(ctx context.Context, req *message.Message) (*message.Message, error) {
				path, _ := req.Path()
				assert.Equal(t, strings.TrimPrefix(tt.expectedPath, "/"), path)
				resp := pool.AcquireMessage(ctx); resp.SetCode(codes.Changed)
				return resp, nil
			}

			err := out.Write(context.Background(), benthosMsg)
			require.NoError(t, err)
			mockConv.AssertExpectations(t) // Ensure MessageToCoAP was called
		})
	}
}
