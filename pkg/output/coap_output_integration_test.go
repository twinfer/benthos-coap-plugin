//go:build integration

package output

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mockserver "github.com/twinfer/benthos-coap-plugin/pkg/testing" // Our mock server package

	coapMessage "github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
)

func newCoapOutputFromConf(t *testing.T, confYaml string) *Output {
	t.Helper()
	// Create a temporary config spec since the real one is in init()
	configSpec := service.NewConfigSpec().
		Field(service.NewStringListField("endpoints")).
		Field(service.NewStringField("default_path").Default("/")).
		Field(service.NewStringField("protocol").Default("udp")).
		Field(service.NewObjectField("security",
			service.NewStringField("mode").Default("none"),
			service.NewStringField("psk_identity").Optional(),
			service.NewStringField("psk_key").Optional(),
			service.NewStringField("cert_file").Optional(),
			service.NewStringField("key_file").Optional(),
			service.NewStringField("ca_cert_file").Optional(),
			service.NewBoolField("require_client_cert").Default(false)).Optional()).
		Field(service.NewObjectField("connection_pool",
			service.NewIntField("max_size").Default(5),
			service.NewDurationField("idle_timeout").Default("30s"),
			service.NewDurationField("health_check_interval").Default("10s"),
			service.NewDurationField("connect_timeout").Default("10s")).Optional()).
		Field(service.NewObjectField("request_options",
			service.NewBoolField("confirmable").Default(true),
			service.NewStringField("default_method").Default("POST"),
			service.NewDurationField("timeout").Default("30s"),
			service.NewStringField("content_format").Default("application/json").Optional(),
			service.NewBoolField("auto_detect_format").Default(true)).Optional()).
		Field(service.NewObjectField("retry_policy",
			service.NewIntField("max_retries").Default(3),
			service.NewDurationField("initial_interval").Default("1s"),
			service.NewDurationField("max_interval").Default("60s"),
			service.NewFloatField("multiplier").Default(2.0),
			service.NewBoolField("jitter").Default(true)).Optional()).
		Field(service.NewObjectField("converter",
			service.NewStringField("default_content_format").Default("application/json"),
			service.NewBoolField("compression_enabled").Default(true),
			service.NewIntField("max_payload_size").Default(1048576),
			service.NewBoolField("preserve_options").Default(false)).Optional())
	
	pConf, err := configSpec.ParseYAML(confYaml, nil)
	require.NoError(t, err)

	output, err := newCoAPOutput(pConf, service.MockResources())
	require.NoError(t, err)
	return output.(*Output)
}

// setupCoapOutput is a helper that creates an Output and connects it
func setupCoapOutput(t *testing.T, confYaml string) (*Output, func()) {
	t.Helper()
	output := newCoapOutputFromConf(t, confYaml)
	ctx := context.Background()
	err := output.Connect(ctx)
	require.NoError(t, err)
	return output, func() {
		output.Close(ctx)
	}
}

func TestCoapOutput_Integration_BasicPostDefaultPath(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	// Add a resource at the path we'll be posting to
	mockServer.AddResource("/default/postpath", coapMessage.TextPlain, []byte("initial data"), false)

	conf := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "/default/postpath"
request_options:
  default_method: "POST"
  content_format: "text/plain"
connection_pool:
  health_check_interval: "24h"
`, mockServer.Addr())

	output, outputCleanup := setupCoapOutput(t, conf)
	defer outputCleanup()

	msgContent := "Hello, Benthos to CoAP!"
	benthosMsg := service.NewMessage([]byte(msgContent))

	err := output.WriteBatch(context.Background(), service.MessageBatch{benthosMsg})
	require.NoError(t, err, "CoapOutput failed to write message")

	history := mockServer.GetRequestHistory()
	require.Len(t, history, 1, "MockCoAPServer should have received one request")

	receivedReq := history[0]
	assert.Equal(t, codes.POST, receivedReq.Code, "Request method should be POST")
	assert.Equal(t, "/default/postpath", receivedReq.Path, "Request path should match default_path")
	assert.Equal(t, []byte(msgContent), receivedReq.Payload, "Payload mismatch")
	contentType, err := receivedReq.Options.ContentFormat()
	require.NoError(t, err)
	assert.Equal(t, coapMessage.TextPlain, contentType, "ContentFormat mismatch")
}

func TestCoapOutput_Integration_PathFromURLAndMetadata(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	// Path in URL - simulate path being extracted from URL by setting it as default_path
	confURLPath := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "/url/path"
request_options:
  default_method: "POST"
`, mockServer.Addr())

	outputURL, cleanupURL := setupCoapOutput(t, confURLPath)
	defer cleanupURL()

	msgContent1 := "Message to URL path"
	err := outputURL.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte(msgContent1))})
	require.NoError(t, err)

	history := mockServer.GetRequestHistory()
	require.Len(t, history, 1)
	assert.Equal(t, "/url/path", history[0].Path, "Path should be from URL")
	mockServer.ClearRequestHistory()

	// Path from metadata (overrides URL path)
	benthosMsgMeta := service.NewMessage([]byte("Message to metadata path"))
	benthosMsgMeta.MetaSet("coap_path", "/metadata/path")
	err = outputURL.WriteBatch(context.Background(), service.MessageBatch{benthosMsgMeta})
	require.NoError(t, err)

	history = mockServer.GetRequestHistory()
	require.Len(t, history, 1)
	assert.Equal(t, "/metadata/path", history[0].Path, "Path should be from metadata")
}

func TestCoapOutput_Integration_DifferentMethods(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	// Add a resource at the path we'll be testing
	mockServer.AddResource("/test/methods", coapMessage.TextPlain, []byte("test data"), false)

	conf := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "/test/methods"
request_options:
  default_method: "GET"
`, mockServer.Addr())
	output, outputCleanup := setupCoapOutput(t, conf)
	defer outputCleanup()

	// 1. Default GET (from config)
	msgGet := service.NewMessage(nil) // No payload for GET
	err := output.WriteBatch(context.Background(), service.MessageBatch{msgGet})
	require.NoError(t, err)

	// 2. PUT from metadata
	msgPut := service.NewMessage([]byte("data for PUT"))
	msgPut.MetaSet("coap_method", "PUT")
	err = output.WriteBatch(context.Background(), service.MessageBatch{msgPut})
	require.NoError(t, err)

	// 3. DELETE from metadata
	msgDelete := service.NewMessage(nil)
	msgDelete.MetaSet("coap_method", "DELETE")
	err = output.WriteBatch(context.Background(), service.MessageBatch{msgDelete})
	require.NoError(t, err)

	history := mockServer.GetRequestHistory()
	require.Len(t, history, 3, "Expected 3 requests")

	assert.Equal(t, codes.GET, history[0].Code, "Method for request 1 should be GET")
	assert.Equal(t, codes.PUT, history[1].Code, "Method for request 2 should be PUT")
	assert.Equal(t, []byte("data for PUT"), history[1].Payload)
	assert.Equal(t, codes.DELETE, history[2].Code, "Method for request 3 should be DELETE")
}

func TestCoapOutput_Integration_ConfirmableNonConfirmable(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	// Add a resource at the path we'll be testing
	mockServer.AddResource("/confirmabletest", coapMessage.TextPlain, []byte("test data"), false)

	baseConf := `
endpoints: ["coap://%s"]
default_path: "/confirmabletest"
`
	// Test Confirmable (default)
	confConfirmable := fmt.Sprintf(baseConf, mockServer.Addr()) // confirmable_messages defaults to true
	outputConf, cleanupConf := setupCoapOutput(t, confConfirmable)

	err := outputConf.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte("confirm this"))})
	require.NoError(t, err)
	cleanupConf()

	historyConf := mockServer.GetRequestHistory()
	require.Len(t, historyConf, 1)
	assert.Equal(t, coapMessage.Confirmable, historyConf[0].Type, "Message should be Confirmable by default")
	mockServer.ClearRequestHistory()

	// Test Non-Confirmable
	confNonConfirmable := fmt.Sprintf(baseConf+`
request_options:
  confirmable: false
`, mockServer.Addr())
	outputNonConf, cleanupNonConf := setupCoapOutput(t, confNonConfirmable)

	err = outputNonConf.WriteBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte("non-confirm this"))})
	require.NoError(t, err)
	cleanupNonConf()

	historyNonConf := mockServer.GetRequestHistory()
	require.Len(t, historyNonConf, 1)
	assert.Equal(t, coapMessage.NonConfirmable, historyNonConf[0].Type, "Message should be NonConfirmable")
}

func TestCoapOutput_Integration_ContentFormatHandling(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	path := "/contenttype"

	tests := []struct {
		name                 string
		configFormat         string // Value for default_content_format in config
		metadataFormat       string // Value for coap_content_format in metadata
		payload              string
		expectedServerFormat coapMessage.MediaType
	}{
		{
			name:                 "config_text_plain",
			configFormat:         "text/plain",
			payload:              "hello",
			expectedServerFormat: coapMessage.TextPlain,
		},
		{
			name:                 "config_app_json",
			configFormat:         "application/json",
			payload:              `{"foo":"bar"}`,
			expectedServerFormat: coapMessage.AppJSON,
		},
		{
			name:                 "metadata_overrides_config_xml",
			configFormat:         "text/plain",
			metadataFormat:       "application/xml",
			payload:              "<data>test</data>",
			expectedServerFormat: coapMessage.AppXML,
		},
		{
			name:                 "no_format_specified_defaults_to_plugin_default", // which is application/json
			payload:              "default case",
			expectedServerFormat: coapMessage.AppJSON,
		},
		{
			name:                 "empty_metadata_uses_config_format",
			configFormat:         "application/octet-stream",
			metadataFormat:       "", // Explicitly empty
			payload:              "binarydata",
			expectedServerFormat: coapMessage.AppOctets,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockServer.ClearRequestHistory()

			confYAML := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "%s"
request_options:
  default_method: "POST"
`, mockServer.Addr(), path)
			if tc.configFormat != "" {
				confYAML += fmt.Sprintf("  content_format: \"%s\"\n", tc.configFormat)
			}

			output, cleanup := setupCoapOutput(t, confYAML)
			defer cleanup()

			msg := service.NewMessage([]byte(tc.payload))
			if tc.metadataFormat != "" { // Only set if tc.metadataFormat is not empty
				msg.MetaSet("content_type", tc.metadataFormat)
			} else if tc.metadataFormat == "" && tc.name == "empty_metadata_uses_config_format" {
				// Special case to test explicit empty metadata value
				msg.MetaSet("content_type", "")
			}

			err := output.WriteBatch(context.Background(), service.MessageBatch{msg})
			require.NoError(t, err)

			history := mockServer.GetRequestHistory()
			require.Len(t, history, 1)
			req := history[0]

			actualFormat, err := req.Options.ContentFormat()
			require.NoError(t, err, "Error getting content format from request")
			assert.Equal(t, tc.expectedServerFormat, actualFormat, "ContentFormat mismatch")
		})
	}
}

// --- Tests requiring MockResource enhancements (Delay, ResponseSequence) ---

func TestCoapOutput_Integration_RequestTimeout(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	timeoutPath := "/timeout"
	resourceData := []byte("some data")

	// Configure resource to delay
	mockServer.AddResource(timeoutPath, coapMessage.TextPlain, resourceData, false)
	delayErr := mockServer.SetResourceDelay(timeoutPath, 200*time.Millisecond) // Delay longer than client timeout
	require.NoError(t, delayErr)

	conf := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "%s"
request_options:
  timeout: "50ms"
retry_policy:
  max_retries: 0
`, mockServer.Addr(), timeoutPath)

	output, outputCleanup := setupCoapOutput(t, conf)
	defer outputCleanup()

	msgContent := "timeout test"
	benthosMsg := service.NewMessage([]byte(msgContent))

	err := output.WriteBatch(context.Background(), service.MessageBatch{benthosMsg})
	require.Error(t, err, "Expected an error due to CoAP request timeout")
	// Check if the error is context.DeadlineExceeded or contains a timeout message
	// This depends on how the go-coap client and Benthos error wrapping behave.
	// For now, any error is accepted as a pass for timeout.
	t.Logf("Timeout error: %v", err)

	history := mockServer.GetRequestHistory()
	// With max_retries: 0 for Benthos, and CON messages (default for CoAP client),
	// the go-coap client itself might retry a few times before its own timeout.
	// If it were NON, it would likely be 1.
	assert.GreaterOrEqual(t, len(history), 1, "Server should have received at least one attempt")
}

// Helper to set resource with response sequence on MockCoAPServer
func setResourceResponseSequence(server *mockserver.MockCoAPServer, path string, seq []codes.Code, data []byte, contentType coapMessage.MediaType, observable bool, opts ...coapMessage.Option) {
	server.MuLock()
	defer server.MuUnlock()

	resource, exists := server.GetResourceInternal(path)
	if !exists {
		resource = &mockserver.MockResource{
			Path:             path,
			Observable:       observable,
			ServeWithOptions: opts,
		}
		server.SetResourceInternal(path, resource)
	}
	resource.ResponseSequence = seq
	resource.Data = data
	resource.ContentType = contentType
	resource.CurrentResponseIndex = 0
}

func TestCoapOutput_Integration_RetryPolicy(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	retryPath := "/retry"
	finalPayload := []byte("final success data") // Data for the successful attempt

	// Configure resource to fail twice then succeed
	setResourceResponseSequence(mockServer, retryPath,
		[]codes.Code{codes.ServiceUnavailable, codes.InternalServerError, codes.Changed}, // Fail, Fail, Success
		finalPayload, // Data associated with the final successful response
		coapMessage.TextPlain,
		false, // Not observable for this test
	)

	conf := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "%s"
request_options:
  default_method: "PUT"
  content_format: "application/octet-stream"
retry_policy:
  initial_interval: "10ms"
  max_interval: "20ms"
  max_retries: 3
`, mockServer.Addr(), retryPath)

	output, outputCleanup := setupCoapOutput(t, conf)
	defer outputCleanup()

	benthosMsg := service.NewMessage([]byte("retry this data"))

	err := output.WriteBatch(context.Background(), service.MessageBatch{benthosMsg})
	require.NoError(t, err, "Expected write to succeed after retries")

	history := mockServer.GetRequestHistory()
	// Expect 3 attempts: Fail (5.03), Fail (5.00), Success (2.04 Changed)
	assert.Len(t, history, 3, "MockCoAPServer should have received three requests due to retries")

	// Check request details (optional, but good for sanity)
	for i, req := range history {
		assert.Equal(t, codes.PUT, req.Code, "Request %d method should be PUT", i)
		assert.Equal(t, []byte("retry this data"), req.Payload, "Request %d payload mismatch", i)
	}

	// Verify the resource data on the server is from the (last) successful PUT
	srvData, exists := mockServer.GetResourceData(retryPath)
	require.True(t, exists, "Resource should exist on server")
	// The mock server's PutHandler updates resource.Data on any PUT if the final code is not an error.
	// If the final code in sequence is Changed, data should be "retry this data".
	// If the ResponseSequence also dictates the payload for errors, then this check needs adjustment.
	// Current MockResource.Data is updated by handler before error code from sequence is returned.
	// This means the data from the last attempt (which succeeded) should be stored.
	assert.Equal(t, []byte("retry this data"), srvData, "Server resource data after retries")
}

func TestCoapOutput_Integration_ServerErrorHandling(t *testing.T) {
	mockServer, serverCleanup := mockserver.SetupTestServer(t)
	defer serverCleanup()

	errorPath := "/permanenterror"

	conf := fmt.Sprintf(`
endpoints: ["coap://%s"]
default_path: "%s"
request_options:
  default_method: "POST"
retry_policy:
  max_retries: 0
`, mockServer.Addr(), errorPath)
	output, outputCleanup := setupCoapOutput(t, conf)
	defer outputCleanup()

	// Scenario 1: Server returns 4.00 Bad Request
	errorPayloadBadRequest := []byte("Details for bad request")
	setResourceResponseSequence(mockServer, errorPath,
		[]codes.Code{codes.BadRequest},
		errorPayloadBadRequest,
		coapMessage.TextPlain, false)

	msgBadRequest := service.NewMessage([]byte("test data for bad request"))

	err := output.WriteBatch(context.Background(), service.MessageBatch{msgBadRequest})
	require.Error(t, err, "Expected an error for 4.00 Bad Request")
	// Note: Benthos might wrap this error. Checking for specific CoAP error codes
	// in the returned error would require deeper inspection or custom error types from the plugin.
	t.Logf("Received error for 4.00: %v", err)

	// Scenario 2: Server returns 5.03 Service Unavailable
	mockServer.ClearRequestHistory() // Clear for next scenario
	errorPayloadServiceUnavail := []byte("Details for service unavailable")
	setResourceResponseSequence(mockServer, errorPath,
		[]codes.Code{codes.ServiceUnavailable},
		errorPayloadServiceUnavail,
		coapMessage.TextPlain, false)

	msgServiceUnavailable := service.NewMessage([]byte("test data for service unavailable"))

	err = output.WriteBatch(context.Background(), service.MessageBatch{msgServiceUnavailable})
	require.Error(t, err, "Expected an error for 5.03 Service Unavailable")
	t.Logf("Received error for 5.03: %v", err)

	history := mockServer.GetRequestHistory()
	// One request for the second scenario (5.03) as history was cleared.
	require.Len(t, history, 1, "Expected one request in history for the 5.03 scenario")
	assert.Equal(t, codes.POST, history[0].Code)
	assert.Equal(t, errorPath, history[0].Path)
}
