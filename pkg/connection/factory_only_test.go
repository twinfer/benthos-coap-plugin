package connection_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twinfer/benthos-coap-plugin/pkg/connection"
)

func TestCreateFactory(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		wantErr  bool
	}{
		{
			name:     "UDP factory",
			protocol: "udp",
			wantErr:  false,
		},
		{
			name:     "TCP factory",
			protocol: "tcp",
			wantErr:  false,
		},
		{
			name:     "DTLS factory",
			protocol: "udp-dtls",
			wantErr:  false,
		},
		{
			name:     "TCP-TLS factory",
			protocol: "tcp-tls",
			wantErr:  false,
		},
		{
			name:     "Unknown protocol",
			protocol: "unknown",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := connection.CreateFactory(tt.protocol)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, factory)
			} else {
				require.NoError(t, err)
				require.NotNil(t, factory)
				assert.Equal(t, tt.protocol, factory.Protocol())
			}
		})
	}
}
