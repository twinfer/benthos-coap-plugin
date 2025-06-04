package input

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/options"
	// Client imports
	coapDtls "github.com/plgd-dev/go-coap/v3/dtls"      // For DTLS client Dial
	tcpClient "github.com/plgd-dev/go-coap/v3/tcp/client"
	udpClient "github.com/plgd-dev/go-coap/v3/udp/client"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to get a free port
func getFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// generateSelfSignedCert creates server cert, key. The server cert is also used as CA.
// Returns paths to certFile, keyFile, caCertFile and a cleanup function.
func generateSelfSignedCert(t *testing.T) (certPath, keyPath, caCertPath string, cleanupFunc func()) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "coap_test_certs_")
	require.NoError(t, err)

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := notBefore.Add(24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Benthos CoAP Test"},
			CommonName:   "localhost",
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certFile := filepath.Join(tempDir, "server.crt.pem")
	keyFile := filepath.Join(tempDir, "server.key.pem")
	caCertFile = certFile // Using server cert as CA for self-signed setup

	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	keyOut.Close()

	return certFile, keyFile, caCertFile, func() { os.RemoveAll(tempDir) }
}

// newTestCoAPServerInput creates a server instance for testing.
func newTestCoAPServerInput(t *testing.T, configYAML string) (*ServerInput, *service.Resources) {
	t.Helper()
	parsedConf, err := coapServerInputConfigSpec.ParseYAML(configYAML, nil) // Assumes coapServerInputConfigSpec is package-level
	require.NoError(t, err, "Failed to parse test config YAML: %s", configYAML)

	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	// To reduce noise: logger.SetLevel(service.LogLevelError)
	mgr := service.MockResources(service.OptSetLogger(logger))

	input, err := newCoAPServerInput(parsedConf, mgr)
	require.NoError(t, err, "Failed to create new CoAP server input")
	serverInput, ok := input.(*ServerInput)
	require.True(t, ok, "Created input is not of type *ServerInput")
	return serverInput, mgr
}

// --- Test Cases ---

func TestCoAPServer_StartStopUDP(t *testing.T) {
	port := getFreePort(t)
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp"
`, port)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := input.Connect(ctx)
	require.NoError(t, err, "Connect should not fail for UDP")
	time.Sleep(50 * time.Millisecond)
	err = input.Close(ctx)
	require.NoError(t, err, "Close should not fail")
}

func TestCoAPServer_StartStopTCP(t *testing.T) {
	port := getFreePort(t)
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "tcp"
`, port)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := input.Connect(ctx)
	require.NoError(t, err, "Connect should not fail for TCP")
	time.Sleep(50 * time.Millisecond)
	err = input.Close(ctx)
	require.NoError(t, err, "Close should not fail")
}

func TestCoAPServer_StartStopTCPTLS(t *testing.T) {
	certFile, keyFile, _, cleanup := generateSelfSignedCert(t)
	defer cleanup()

	port := getFreePort(t)
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "tcp-tls"
security:
  mode: "certificate"
  cert_file: "%s"
  key_file: "%s"
`, port, strings.ReplaceAll(certFile, "\\", "\\\\"), strings.ReplaceAll(keyFile, "\\", "\\\\"))
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := input.Connect(ctx)
	require.NoError(t, err, "Connect should not fail for TCP-TLS")
	time.Sleep(50 * time.Millisecond)
	err = input.Close(ctx)
	require.NoError(t, err, "Close should not fail")
}

func TestCoAPServer_StartStopDTLS_PSK(t *testing.T) {
	port := getFreePort(t)
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp-dtls"
security:
  mode: "psk"
  psk_identity: "test-identity"
  psk_key: "test-key"
`, port)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // Increased timeout for DTLS
	defer cancel()

	err := input.Connect(ctx)
	require.NoError(t, err, "Connect should not fail for DTLS-PSK")
	time.Sleep(100 * time.Millisecond) // Give DTLS a bit more time

	// Optional: Add a basic client dial check if simple
	dtlsConfig := &piondtls.Config{
		PSK: func(hint []byte) ([]byte, error) {
			return []byte("test-key"), nil
		},
		PSKIdentityHint: []byte("test-identity"),
		CipherSuites:    []piondtls.CipherSuiteID{piondtls.TLS_PSK_WITH_AES_128_CCM_8},
	}
	clientCtx, clientCancel := context.WithTimeout(ctx, 2*time.Second)
	defer clientCancel()
	// coapDtls.Dial requires options.WithContext for timeout
	// For just a connection test, this might be enough, or it might need a full client.
	// For now, just testing server start/stop is the primary goal.
	// Let's try a dial.
	conn, err := coapDtls.DialWithContext(clientCtx, fmt.Sprintf("127.0.0.1:%d", port), dtlsConfig)
	if err == nil {
		conn.Close()
	}
	t.Logf("DTLS Dial attempt err: %v (this is informative for PSK setup)", err)
	// Not failing the test on client dial error here, primary is server Start/Stop.

	err = input.Close(ctx)
	require.NoError(t, err, "Close should not fail")
}


func TestCoAPServer_BasicGetUDP(t *testing.T) {
	port := getFreePort(t)
	defaultPayload := "Hello UDP GET"
	targetPath := "/testgetudp"

	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp"
allowed_paths: ["%s"]
response:
  default_payload: "%s"
`, port, targetPath, defaultPayload)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := udpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reqCancel()
	resp, err := client.Get(reqCtx, targetPath)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer pool.ReleaseMessage(resp)

	assert.Equal(t, codes.Content, resp.Code())
	body, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, defaultPayload, string(body))
}

func TestCoAPServer_BasicGetTCP(t *testing.T) {
	port := getFreePort(t)
	defaultPayload := "Hello TCP GET"
	targetPath := "/testgettcp"
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "tcp"
allowed_paths: ["%s"]
response:
  default_payload: "%s"
`, port, targetPath, defaultPayload)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := tcpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reqCancel()
	resp, err := client.Get(reqCtx, targetPath)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer pool.ReleaseMessage(resp)

	assert.Equal(t, codes.Content, resp.Code())
	body, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, defaultPayload, string(body))
}

func TestCoAPServer_BasicPostUDP(t *testing.T) {
	port := getFreePort(t)
	postPayload := "Posting UDP data"
	responsePayload := "UDP POST Handled"
	targetPath := "/submitudp"

	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp"
allowed_paths: ["%s"]
allowed_methods: ["POST"]
response:
  default_payload: "%s"
  default_code: %d
`, port, targetPath, responsePayload, codes.Changed)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := udpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reqCancel()
	resp, err := client.Post(reqCtx, targetPath, message.TextPlain, strings.NewReader(postPayload))
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer pool.ReleaseMessage(resp)
	assert.Equal(t, codes.Changed, resp.Code())

	respBody, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, responsePayload, string(respBody))

	readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
	defer readCancel()
	bMsg, _, err := input.Read(readCtx)
	require.NoError(t, err, "Failed to read message from Benthos input")
	require.NotNil(t, bMsg)

	msgBytes, _ := bMsg.AsBytes()
	assert.Equal(t, postPayload, string(msgBytes))

	metaMethod, _ := bMsg.MetaGet("coap_server_method")
	assert.Equal(t, "POST", metaMethod)
	metaPath, _ := bMsg.MetaGet("coap_server_path")
	assert.Equal(t, targetPath, metaPath)
}

func TestCoAPServer_BasicPostTCP(t *testing.T) {
	port := getFreePort(t)
	postPayload := "Posting TCP data"
	responsePayload := "TCP POST Handled"
	targetPath := "/submittcp"

	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "tcp"
allowed_paths: ["%s"]
allowed_methods: ["POST"]
response:
  default_payload: "%s"
  default_code: %d
`, port, targetPath, responsePayload, codes.Created)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := tcpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	reqCtx, reqCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reqCancel()
	resp, err := client.Post(reqCtx, targetPath, message.AppJSON, strings.NewReader(postPayload))
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer pool.ReleaseMessage(resp)
	assert.Equal(t, codes.Created, resp.Code())

	respBody, err := resp.ReadBody()
	require.NoError(t, err)
	assert.Equal(t, responsePayload, string(respBody))

	readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
	defer readCancel()
	bMsg, _, err := input.Read(readCtx)
	require.NoError(t, err)
	require.NotNil(t, bMsg)
	msgBytes, _ := bMsg.AsBytes()
	assert.Equal(t, postPayload, string(msgBytes))
	metaMethod, _ := bMsg.MetaGet("coap_server_method")
	assert.Equal(t, "POST", metaMethod)
}

func TestCoAPServer_PathFiltering(t *testing.T) {
	port := getFreePort(t)
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp"
allowed_paths: ["/allowed", "/allowed/+"]
response:
  default_payload: "Data"
`, port)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := udpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	testCases := []struct {
		name         string
		path         string
		expectedCode codes.Code
	}{
		{"ExactAllowed", "/allowed", codes.Content},
		{"WildcardSegmentAllowed", "/allowed/foo", codes.Content},
		{"WildcardTooManySegments", "/allowed/foo/bar", codes.NotFound},
		{"DisallowedExact", "/disallowed", codes.NotFound},
		{"RootDisallowed", "/", codes.NotFound},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
			defer reqCancel()
			resp, errPath := client.Get(reqCtx, tc.path)
			require.NoError(t, errPath)
			require.NotNil(t, resp)
			defer pool.ReleaseMessage(resp)
			assert.Equal(t, tc.expectedCode, resp.Code(), "Path: %s", tc.path)
		})
	}
}

func TestCoAPServer_MethodFiltering(t *testing.T) {
	port := getFreePort(t)
	targetPath := "/resource"
	configYAML := fmt.Sprintf(`
listen_address: "127.0.0.1:%d"
protocol: "udp"
allowed_paths: ["%s"]
allowed_methods: ["GET", "DELETE"]
response:
  default_code: %d
`, port, targetPath, codes.Content)
	input, _ := newTestCoAPServerInput(t, configYAML)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)
	time.Sleep(100 * time.Millisecond)

	client, err := udpClient.Dial(fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	defer client.Close()

	t.Run("GET_Allowed", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		defer reqCancel()
		resp, errGet := client.Get(reqCtx, targetPath)
		require.NoError(t, errGet)
		require.NotNil(t, respGet)
		defer pool.ReleaseMessage(respGet)
		assert.Equal(t, codes.Content, respGet.Code())
	})

	t.Run("DELETE_Allowed", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		defer reqCancel()
		resp, errDel := client.Delete(reqCtx, targetPath)
		require.NoError(t, errDel)
		require.NotNil(t, resp)
		defer pool.ReleaseMessage(resp)
		assert.Equal(t, codes.Content, resp.Code())
	})

	t.Run("POST_Disallowed", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		defer reqCancel()
		resp, errPost := client.Post(reqCtx, targetPath, message.TextPlain, strings.NewReader("test"))
		require.NoError(t, errPost)
		require.NotNil(t, respPost)
		defer pool.ReleaseMessage(respPost)
		assert.Equal(t, codes.MethodNotAllowed, respPost.Code())
	})

	t.Run("PUT_Disallowed", func(t *testing.T) {
		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		defer reqCancel()
		resp, errPut := client.Put(reqCtxPut, targetPath, message.TextPlain, strings.NewReader("test"))
		require.NoError(t, errPut)
		require.NotNil(t, respPut)
		defer pool.ReleaseMessage(respPut)
		assert.Equal(t, codes.MethodNotAllowed, respPut.Code())
	})
}
