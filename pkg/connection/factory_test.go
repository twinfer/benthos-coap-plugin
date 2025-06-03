package connection

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test the factories without the complexity of metrics
func TestFactoryCreation(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		factory  ConnectionFactory
	}{
		{
			name:     "UDP Factory",
			protocol: "udp",
			factory:  &UDPFactory{},
		},
		{
			name:     "TCP Factory",
			protocol: "tcp",
			factory:  &TCPFactory{},
		},
		{
			name:     "DTLS Factory",
			protocol: "udp-dtls",
			factory:  &DTLSFactory{},
		},
		{
			name:     "TCP-TLS Factory",
			protocol: "tcp-tls",
			factory:  &TCPTLSFactory{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.protocol, tt.factory.Protocol())
		})
	}
}

func TestDTLSConfigCreation(t *testing.T) {
	factory := &DTLSFactory{}

	t.Run("Valid PSK Config", func(t *testing.T) {
		config := SecurityConfig{
			Mode:        "psk",
			PSKIdentity: "client1",
			PSKKey:      "secretkey",
		}
		
		dtlsConfig, err := factory.createDTLSConfig(config)
		require.NoError(t, err)
		require.NotNil(t, dtlsConfig)
		assert.NotNil(t, dtlsConfig.PSK)
		assert.Equal(t, []byte("client1"), dtlsConfig.PSKIdentityHint)
		assert.Len(t, dtlsConfig.CipherSuites, 3)
	})

	t.Run("Invalid PSK Config - Missing Identity", func(t *testing.T) {
		config := SecurityConfig{
			Mode:   "psk",
			PSKKey: "secretkey",
		}
		
		_, err := factory.createDTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "PSK mode requires both psk_key and psk_identity")
	})

	t.Run("Invalid PSK Config - Missing Key", func(t *testing.T) {
		config := SecurityConfig{
			Mode:        "psk",
			PSKIdentity: "client1",
		}
		
		_, err := factory.createDTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "PSK mode requires both psk_key and psk_identity")
	})

	t.Run("Unsupported Mode", func(t *testing.T) {
		config := SecurityConfig{
			Mode: "oauth2",
		}
		
		_, err := factory.createDTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported security mode for DTLS")
	})
}

func TestTCPTLSConfigCreation(t *testing.T) {
	factory := &TCPTLSFactory{}

	t.Run("None Mode", func(t *testing.T) {
		config := SecurityConfig{
			Mode: "none",
		}
		
		tlsConfig, err := factory.createTLSConfig(config)
		require.NoError(t, err)
		require.NotNil(t, tlsConfig)
		assert.False(t, tlsConfig.InsecureSkipVerify)
	})

	t.Run("Certificate Mode - Missing Files", func(t *testing.T) {
		config := SecurityConfig{
			Mode:     "certificate",
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  "/nonexistent/key.pem",
		}
		
		_, err := factory.createTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load certificate pair")
	})

	t.Run("Certificate Mode - Missing Key", func(t *testing.T) {
		config := SecurityConfig{
			Mode:     "certificate",
			CertFile: "cert.pem",
		}
		
		_, err := factory.createTLSConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "certificate mode requires both cert_file and key_file")
	})

	t.Run("Insecure Skip Verify", func(t *testing.T) {
		config := SecurityConfig{
			Mode:         "none",
			InsecureSkip: true,
		}
		
		tlsConfig, err := factory.createTLSConfig(config)
		require.NoError(t, err)
		assert.True(t, tlsConfig.InsecureSkipVerify)
	})
}