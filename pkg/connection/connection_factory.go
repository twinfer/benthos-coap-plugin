// pkg/connection/factory.go
package connection

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/pion/dtls/v3"
	coapDTLS "github.com/plgd-dev/go-coap/v3/dtls"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/tcp"
	coapTCPClient "github.com/plgd-dev/go-coap/v3/tcp/client"
	"github.com/plgd-dev/go-coap/v3/udp"
	"github.com/plgd-dev/go-coap/v3/udp/client"
)

type ConnectionFactory interface {
	Create(endpoint string, security SecurityConfig) (any, error)
	Validate(conn any) error
	Close(conn any) error
	Protocol() string
}

// UDP Factory
type UDPFactory struct{}

func (f *UDPFactory) Protocol() string {
	return "udp"
}

func (f *UDPFactory) Create(endpoint string, security SecurityConfig) (any, error) {
	return udp.Dial(endpoint)
}

func (f *UDPFactory) Validate(conn any) error {
	udpConn, ok := conn.(*client.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type for UDP factory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simple ping using empty confirmable message
	return udpConn.Ping(ctx)
}

func (f *UDPFactory) Close(conn any) error {
	if udpConn, ok := conn.(*client.Conn); ok {
		return udpConn.Close()
	}
	return fmt.Errorf("invalid connection type for UDP factory")
}

// TCP Factory
type TCPFactory struct{}

func (f *TCPFactory) Protocol() string {
	return "tcp"
}

func (f *TCPFactory) Create(endpoint string, security SecurityConfig) (any, error) {
	// tcp.Dial returns (*client.Conn, error)
	return tcp.Dial(endpoint)
}

func (f *TCPFactory) Validate(conn any) error {
	tcpConn, ok := conn.(*coapTCPClient.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type for TCP factory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if connection context is still valid
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tcpConn.Context().Done():
		return fmt.Errorf("connection closed")
	default:
		return nil
	}
}

func (f *TCPFactory) Close(conn any) error {
	if tcpConn, ok := conn.(*coapTCPClient.Conn); ok {
		return tcpConn.Close()
	}
	return fmt.Errorf("invalid connection type for TCP factory")
}

// DTLS Factory
type DTLSFactory struct{}

func (f *DTLSFactory) Protocol() string {
	return "udp-dtls"
}

func (f *DTLSFactory) Create(endpoint string, security SecurityConfig) (any, error) {
	config, err := f.createDTLSConfig(security)
	if err != nil {
		return nil, fmt.Errorf("failed to create DTLS config: %w", err)
	}

	return coapDTLS.Dial(endpoint, config)
}

func (f *DTLSFactory) createDTLSConfig(security SecurityConfig) (*dtls.Config, error) {
	config := &dtls.Config{}

	switch security.Mode {
	case "psk":
		if security.PSKKey == "" || security.PSKIdentity == "" {
			return nil, fmt.Errorf("PSK mode requires both psk_key and psk_identity")
		}

		config.PSK = func(hint []byte) ([]byte, error) {
			return []byte(security.PSKKey), nil
		}
		config.PSKIdentityHint = []byte(security.PSKIdentity)
		config.CipherSuites = []dtls.CipherSuiteID{
			dtls.TLS_PSK_WITH_AES_128_CCM,
			dtls.TLS_PSK_WITH_AES_128_CCM_8,
			dtls.TLS_PSK_WITH_AES_256_CCM_8,
		}

	case "certificate":
		if security.CertFile == "" || security.KeyFile == "" {
			return nil, fmt.Errorf("certificate mode requires both cert_file and key_file")
		}

		cert, err := tls.LoadX509KeyPair(security.CertFile, security.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate pair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}

		// Load CA certificates if provided
		if security.CACertFile != "" {
			caCertPEM, err := os.ReadFile(security.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCertPEM) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			config.RootCAs = caCertPool
		}

		config.InsecureSkipVerify = security.InsecureSkip

	default:
		return nil, fmt.Errorf("unsupported security mode for DTLS: %s", security.Mode)
	}

	return config, nil
}

func (f *DTLSFactory) Validate(conn any) error {
	udpConn, ok := conn.(*client.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type for DTLS factory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if connection context is still valid
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-udpConn.Context().Done():
		return fmt.Errorf("connection closed")
	default:
		return nil
	}
}

func (f *DTLSFactory) Close(conn any) error {
	if udpConn, ok := conn.(*client.Conn); ok {
		return udpConn.Close()
	}
	return fmt.Errorf("invalid connection type for DTLS factory")
}

// TCP-TLS Factory
type TCPTLSFactory struct{}

func (f *TCPTLSFactory) Protocol() string {
	return "tcp-tls"
}

func (f *TCPTLSFactory) Create(endpoint string, security SecurityConfig) (any, error) {
	config, err := f.createTLSConfig(security)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %w", err)
	}

	return tcp.Dial(endpoint, options.WithTLS(config))
}

func (f *TCPTLSFactory) createTLSConfig(security SecurityConfig) (*tls.Config, error) {
	config := &tls.Config{
		InsecureSkipVerify: security.InsecureSkip,
	}

	switch security.Mode {
	case "certificate":
		if security.CertFile == "" || security.KeyFile == "" {
			return nil, fmt.Errorf("certificate mode requires both cert_file and key_file")
		}

		cert, err := tls.LoadX509KeyPair(security.CertFile, security.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate pair: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}

		// Load CA certificates if provided
		if security.CACertFile != "" {
			caCertPEM, err := os.ReadFile(security.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCertPEM) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			config.RootCAs = caCertPool
		}

	case "none":
		// No additional configuration needed for plain TLS

	default:
		return nil, fmt.Errorf("unsupported security mode for TCP-TLS: %s (use 'certificate' or 'none')", security.Mode)
	}

	return config, nil
}

func (f *TCPTLSFactory) Validate(conn any) error {
	tcpConn, ok := conn.(*coapTCPClient.Conn)
	if !ok {
		return fmt.Errorf("invalid connection type for TCP-TLS factory")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if connection context is still valid
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tcpConn.Context().Done():
		return fmt.Errorf("connection closed")
	default:
		return nil
	}
}

func (f *TCPTLSFactory) Close(conn any) error {
	if tcpConn, ok := conn.(*coapTCPClient.Conn); ok {
		return tcpConn.Close()
	}
	return fmt.Errorf("invalid connection type for TCP-TLS factory")
}

// Factory creation function
func CreateFactory(protocol string) (ConnectionFactory, error) {
	switch protocol {
	case "udp":
		return &UDPFactory{}, nil
	case "tcp":
		return &TCPFactory{}, nil
	case "udp-dtls":
		return &DTLSFactory{}, nil
	case "tcp-tls":
		return &TCPTLSFactory{}, nil
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", protocol)
	}
}
