package connection

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test server helpers - creates a simple UDP echo server for testing
func startTestUDPServer(t *testing.T, port int) (net.Addr, func()) {
	t.Helper()
	
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	
	conn, err := net.ListenUDP("udp", addr)
	require.NoError(t, err)
	
	done := make(chan bool)
	
	go func() {
		buf := make([]byte, 1024)
		for {
			select {
			case <-done:
				return
			default:
				conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				n, addr, err := conn.ReadFromUDP(buf)
				if err != nil {
					continue
				}
				// Echo back
				conn.WriteToUDP(buf[:n], addr)
			}
		}
	}()
	
	// Wait for server to start
	time.Sleep(50 * time.Millisecond)
	
	return conn.LocalAddr(), func() {
		close(done)
		conn.Close()
	}
}

func TestCreateFactory(t *testing.T) {
	tests := []struct {
		name        string
		protocol    string
		wantFactory string
		wantErr     bool
	}{
		{
			name:        "UDP factory",
			protocol:    "udp",
			wantFactory: "*connection.UDPFactory",
			wantErr:     false,
		},
		{
			name:        "TCP factory",
			protocol:    "tcp",
			wantFactory: "*connection.TCPFactory",
			wantErr:     false,
		},
		{
			name:        "DTLS factory",
			protocol:    "udp-dtls",
			wantFactory: "*connection.DTLSFactory",
			wantErr:     false,
		},
		{
			name:        "TCP-TLS factory",
			protocol:    "tcp-tls",
			wantFactory: "*connection.TCPTLSFactory",
			wantErr:     false,
		},
		{
			name:        "Unknown protocol",
			protocol:    "unknown",
			wantFactory: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := CreateFactory(tt.protocol)
			
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, factory)
				assert.Contains(t, err.Error(), "unsupported protocol")
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, factory)
				assert.Equal(t, tt.protocol, factory.Protocol())
				assert.Equal(t, tt.wantFactory, fmt.Sprintf("%T", factory))
			}
		})
	}
}

func TestUDPFactory(t *testing.T) {
	factory := &UDPFactory{}

	t.Run("Protocol", func(t *testing.T) {
		assert.Equal(t, "udp", factory.Protocol())
	})

	t.Run("Create", func(t *testing.T) {
		addr, cleanup := startTestUDPServer(t, 5683)
		defer cleanup()

		conn, err := factory.Create(addr.String(), SecurityConfig{})
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Cleanup
		err = factory.Close(conn)
		assert.NoError(t, err)
	})

	t.Run("Validate", func(t *testing.T) {
		addr, cleanup := startTestUDPServer(t, 5684)
		defer cleanup()

		conn, err := factory.Create(addr.String(), SecurityConfig{})
		require.NoError(t, err)

		// Test valid connection
		err = factory.Validate(conn)
		assert.NoError(t, err)

		// Test invalid connection type
		err = factory.Validate("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")

		// Cleanup
		factory.Close(conn)
	})

	t.Run("Close", func(t *testing.T) {
		addr, cleanup := startTestUDPServer(t, 5685)
		defer cleanup()

		conn, err := factory.Create(addr.String(), SecurityConfig{})
		require.NoError(t, err)

		// Test close
		err = factory.Close(conn)
		assert.NoError(t, err)

		// Test close with invalid type
		err = factory.Close("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})
}

func TestTCPFactory(t *testing.T) {
	factory := &TCPFactory{}

	t.Run("Protocol", func(t *testing.T) {
		assert.Equal(t, "tcp", factory.Protocol())
	})

	t.Run("Validate with invalid type", func(t *testing.T) {
		err := factory.Validate("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})

	t.Run("Close with invalid type", func(t *testing.T) {
		err := factory.Close("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})
}

func TestDTLSFactory(t *testing.T) {
	factory := &DTLSFactory{}

	t.Run("Protocol", func(t *testing.T) {
		assert.Equal(t, "udp-dtls", factory.Protocol())
	})

	t.Run("Create with invalid PSK config", func(t *testing.T) {
		security := SecurityConfig{
			Mode:        "psk",
			PSKIdentity: "test",
			// Missing PSKKey
		}

		conn, err := factory.Create("localhost:5686", security)
		assert.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "PSK mode requires both psk_key and psk_identity")
	})

	t.Run("Create with invalid certificate config", func(t *testing.T) {
		security := SecurityConfig{
			Mode:     "certificate",
			CertFile: "test.crt",
			// Missing KeyFile
		}

		conn, err := factory.Create("localhost:5687", security)
		assert.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "certificate mode requires both cert_file and key_file")
	})

	t.Run("Create with unsupported security mode", func(t *testing.T) {
		security := SecurityConfig{
			Mode: "unsupported",
		}

		conn, err := factory.Create("localhost:5688", security)
		assert.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "unsupported security mode for DTLS")
	})

	t.Run("Validate with invalid type", func(t *testing.T) {
		err := factory.Validate("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})

	t.Run("Close with invalid type", func(t *testing.T) {
		err := factory.Close("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})
}

func TestTCPTLSFactory(t *testing.T) {
	factory := &TCPTLSFactory{}

	t.Run("Protocol", func(t *testing.T) {
		assert.Equal(t, "tcp-tls", factory.Protocol())
	})

	t.Run("Create with invalid certificate config", func(t *testing.T) {
		security := SecurityConfig{
			Mode:     "certificate",
			CertFile: "test.crt",
			// Missing KeyFile
		}

		conn, err := factory.Create("localhost:5689", security)
		assert.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "certificate mode requires both cert_file and key_file")
	})

	t.Run("Create with none security mode", func(t *testing.T) {
		security := SecurityConfig{
			Mode: "none",
		}

		// This should not error in config creation
		config, err := factory.createTLSConfig(security)
		assert.NoError(t, err)
		assert.NotNil(t, config)
	})

	t.Run("Create with unsupported security mode", func(t *testing.T) {
		security := SecurityConfig{
			Mode: "psk", // PSK not supported for TCP-TLS
		}

		conn, err := factory.Create("localhost:5690", security)
		assert.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "unsupported security mode for TCP-TLS")
	})

	t.Run("Validate with invalid type", func(t *testing.T) {
		err := factory.Validate("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})

	t.Run("Close with invalid type", func(t *testing.T) {
		err := factory.Close("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type")
	})
}

func TestSecurityConfig(t *testing.T) {
	t.Run("DTLS PSK Config", func(t *testing.T) {
		factory := &DTLSFactory{}
		security := SecurityConfig{
			Mode:        "psk",
			PSKIdentity: "test-id",
			PSKKey:      "test-key",
		}

		config, err := factory.createDTLSConfig(security)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.NotNil(t, config.PSK)
		assert.Equal(t, []byte("test-id"), config.PSKIdentityHint)
		assert.Len(t, config.CipherSuites, 3)
	})

	t.Run("TLS Certificate Config with CA", func(t *testing.T) {
		factory := &TCPTLSFactory{}
		
		// Create temporary test files
		certFile := "/tmp/test.crt"
		keyFile := "/tmp/test.key"
		caFile := "/tmp/ca.crt"
		
		// Note: Testing with non-existent files to verify error handling
		// In real tests, you would create temporary test certificates
		
		security := SecurityConfig{
			Mode:         "certificate",
			CertFile:     certFile,
			KeyFile:      keyFile,
			CACertFile:   caFile,
			InsecureSkip: true,
		}

		// Test will fail with file not found, which is expected
		config, err := factory.createTLSConfig(security)
		assert.Error(t, err) // Expected: failed to load certificate pair
		assert.Nil(t, config)
	})
}

// Benchmark tests
func BenchmarkCreateFactory(b *testing.B) {
	protocols := []string{"udp", "tcp", "udp-dtls", "tcp-tls"}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		protocol := protocols[i%len(protocols)]
		factory, err := CreateFactory(protocol)
		if err != nil {
			b.Fatal(err)
		}
		_ = factory.Protocol()
	}
}

func BenchmarkUDPConnection(b *testing.B) {
	// Create a simple test helper for benchmarks
	addr, err := net.ResolveUDPAddr("udp", "localhost:5691")
	require.NoError(b, err)
	
	serverConn, err := net.ListenUDP("udp", addr)
	require.NoError(b, err)
	defer serverConn.Close()
	
	// Start echo server
	done := make(chan bool)
	go func() {
		buf := make([]byte, 1024)
		for {
			select {
			case <-done:
				return
			default:
				serverConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				n, addr, err := serverConn.ReadFromUDP(buf)
				if err != nil {
					continue
				}
				serverConn.WriteToUDP(buf[:n], addr)
			}
		}
	}()
	defer close(done)
	
	time.Sleep(50 * time.Millisecond)
	
	factory := &UDPFactory{}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := factory.Create(serverConn.LocalAddr().String(), SecurityConfig{})
		if err != nil {
			b.Fatal(err)
		}
		
		if err := factory.Validate(conn); err != nil {
			b.Fatal(err)
		}
		
		if err := factory.Close(conn); err != nil {
			b.Fatal(err)
		}
	}
}

// Mock connection for testing validation with closed contexts
type mockConn struct {
	ctx context.Context
}

func (m *mockConn) Context() context.Context {
	return m.ctx
}

func (m *mockConn) Close() error {
	return nil
}

func TestConnectionValidationWithContext(t *testing.T) {
	t.Run("TCP factory rejects invalid connection type", func(t *testing.T) {
		factory := &TCPFactory{}
		
		// Create a mock connection of wrong type
		mockConnection := &mockConn{ctx: context.Background()}
		
		// This should fail because mockConn is not *coapTCPClient.Conn
		err := factory.Validate(mockConnection)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type for TCP factory")
	})
	
	t.Run("DTLS factory rejects invalid connection type", func(t *testing.T) {
		factory := &DTLSFactory{}
		
		// This should fail because string is not *client.Conn
		err := factory.Validate("not a connection")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid connection type for DTLS factory")
	})
}