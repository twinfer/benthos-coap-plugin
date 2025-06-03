// pkg/converter/converter.go
package converter

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type Converter struct {
	config Config
	logger *service.Logger
}

type Config struct {
	DefaultContentFormat string `yaml:"default_content_format"`
	CompressionEnabled   bool   `yaml:"compression_enabled"`
	MaxPayloadSize       int    `yaml:"max_payload_size"`
	PreserveOptions      bool   `yaml:"preserve_options"`
}

const (
	// Common CoAP Content-Format values
	TextPlain     = 0
	AppLinkFormat = 40
	AppXML        = 41
	AppOctets     = 42
	AppJSON       = 50
	AppCBOR       = 60
)

func NewConverter(config Config, logger *service.Logger) *Converter {
	if config.MaxPayloadSize == 0 {
		config.MaxPayloadSize = 1024 * 1024 // 1MB default
	}

	return &Converter{
		config: config,
		logger: logger,
	}
}

func (c *Converter) CoAPToMessage(coapMsg *message.Message) (*service.Message, error) {
	if coapMsg == nil {
		return nil, fmt.Errorf("CoAP message is nil")
	}

	// Extract payload
	payload, err := c.extractPayload(coapMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to extract payload: %w", err)
	}

	// Create Benthos message
	msg := service.NewMessage(payload)

	// Add CoAP metadata
	c.addCoAPMetadata(msg, coapMsg)

	// Handle content format specific processing
	if err := c.processContentFormat(msg, coapMsg); err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to process content format: %v", err))
	}

	return msg, nil
}

func (c *Converter) MessageToCoAP(msg *service.Message) (*message.Message, error) {
	if msg == nil {
		return nil, fmt.Errorf("Benthos message is nil")
	}

	// Create CoAP message
	coapMsg := &message.Message{}

	// Set message type to Confirmable by default
	coapMsg.Type = message.Confirmable

	// Set method from metadata or default to POST
	method := c.getMethodFromMetadata(msg)
	coapMsg.Code = method

	// Set content format - we'll need to handle this through Options
	_, err := c.determineContentFormat(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to determine content format: %w", err)
	}
	// TODO: Set content format through Options with proper buffer management

	// Set payload
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get message payload: %w", err)
	}

	if c.config.CompressionEnabled && len(payload) > 1024 {
		compressed, err := c.compressPayload(payload)
		if err != nil {
			c.logger.Warnf("Failed to compress payload: %v", err)
		} else if len(compressed) < len(payload) {
			payload = compressed
			// Note: ContentEncoding is not a standard CoAP option in go-coap v3
			// We'll add it as a custom option if needed
		}
	}

	if len(payload) > c.config.MaxPayloadSize {
		return nil, fmt.Errorf("payload size %d exceeds maximum %d", len(payload), c.config.MaxPayloadSize)
	}

	coapMsg.Payload = payload

	// Add options from metadata
	c.addOptionsFromMetadata(coapMsg, msg)

	// Set token from metadata if available
	if tokenStr, exists := msg.MetaGet("coap_token"); exists {
		coapMsg.Token = message.Token(tokenStr)
	}

	return coapMsg, nil
}

func (c *Converter) extractPayload(coapMsg *message.Message) ([]byte, error) {
	payload := coapMsg.Payload

	// Check for compression (ContentEncoding is not standard in CoAP but may be used as custom option)
	// For now, we'll skip compression detection and return raw payload
	// In a real implementation, you might use a custom option number for content encoding

	return payload, nil
}

func (c *Converter) addCoAPMetadata(msg *service.Message, coapMsg *message.Message) {
	// Basic message info
	msg.MetaSet("coap_code", coapMsg.Code.String())
	msg.MetaSet("coap_token", string(coapMsg.Token))
	msg.MetaSet("coap_message_id", strconv.Itoa(int(coapMsg.MessageID)))
	msg.MetaSet("coap_type", coapMsg.Type.String())

	// Content format
	if cf, err := coapMsg.Options.GetUint32(message.ContentFormat); err == nil {
		msg.MetaSet("coap_content_format", strconv.Itoa(int(cf)))
		msg.MetaSet("coap_content_type", c.contentFormatToMimeType(cf))
	}

	// URI path and query
	uriPathBuf := make([]string, 16) // Buffer for path segments
	if pathCount, err := coapMsg.Options.GetStrings(message.URIPath, uriPathBuf); err == nil && pathCount > 0 {
		msg.MetaSet("coap_uri_path", "/"+strings.Join(uriPathBuf[:pathCount], "/"))
	}

	uriQueryBuf := make([]string, 16) // Buffer for query segments
	if queryCount, err := coapMsg.Options.GetStrings(message.URIQuery, uriQueryBuf); err == nil && queryCount > 0 {
		msg.MetaSet("coap_uri_query", strings.Join(uriQueryBuf[:queryCount], "&"))
	}

	// Observe option
	if observe, err := coapMsg.Options.GetUint32(message.Observe); err == nil {
		msg.MetaSet("coap_observe", strconv.Itoa(int(observe)))
	}

	// Max-Age
	if maxAge, err := coapMsg.Options.GetUint32(message.MaxAge); err == nil {
		msg.MetaSet("coap_max_age", strconv.Itoa(int(maxAge)))
	}

	// ETag
	if etag, err := coapMsg.Options.GetBytes(message.ETag); err == nil {
		msg.MetaSet("coap_etag", string(etag))
	}

	// Location path
	locationPathBuf := make([]string, 16) // Buffer for location path segments
	if locationCount, err := coapMsg.Options.GetStrings(message.LocationPath, locationPathBuf); err == nil && locationCount > 0 {
		msg.MetaSet("coap_location_path", "/"+strings.Join(locationPathBuf[:locationCount], "/"))
	}

	// Add all options if preservation is enabled
	if c.config.PreserveOptions {
		c.preserveAllOptions(msg, coapMsg)
	}
}

func (c *Converter) processContentFormat(msg *service.Message, coapMsg *message.Message) error {
	contentFormat, err := coapMsg.Options.GetUint32(message.ContentFormat)
	if err != nil {
		return nil // No content format specified
	}

	switch contentFormat {
	case AppJSON:
		return c.processJSONPayload(msg)
	case AppCBOR:
		return c.processCBORPayload(msg)
	case AppXML:
		return c.processXMLPayload(msg)
	default:
		// Leave as raw bytes
		return nil
	}
}

func (c *Converter) processJSONPayload(msg *service.Message) error {
	payload, err := msg.AsBytes()
	if err != nil {
		return err
	}

	// Validate JSON
	var jsonData interface{}
	if err := json.Unmarshal(payload, &jsonData); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}

	// Set structured data flag
	msg.MetaSet("coap_structured_data", "json")
	return nil
}

func (c *Converter) processCBORPayload(msg *service.Message) error {
	// CBOR processing would go here
	msg.MetaSet("coap_structured_data", "cbor")
	return nil
}

func (c *Converter) processXMLPayload(msg *service.Message) error {
	msg.MetaSet("coap_structured_data", "xml")
	return nil
}

func (c *Converter) getMethodFromMetadata(msg *service.Message) codes.Code {
	if methodStr, exists := msg.MetaGet("coap_method"); exists {
		switch strings.ToUpper(methodStr) {
		case "GET":
			return codes.GET
		case "POST":
			return codes.POST
		case "PUT":
			return codes.PUT
		case "DELETE":
			return codes.DELETE
		}
	}
	return codes.POST // Default
}

func (c *Converter) determineContentFormat(msg *service.Message) (uint32, error) {
	// Check explicit metadata
	if cfStr, exists := msg.MetaGet("coap_content_format"); exists {
		cf, err := strconv.ParseUint(cfStr, 10, 32)
		if err == nil {
			return uint32(cf), nil
		}
	}

	// Check content type metadata
	if ctStr, exists := msg.MetaGet("content_type"); exists {
		return c.mimeTypeToContentFormat(ctStr), nil
	}

	// Try to auto-detect from payload
	payload, err := msg.AsBytes()
	if err != nil {
		return AppOctets, nil
	}

	return c.autoDetectContentFormat(payload), nil
}

func (c *Converter) autoDetectContentFormat(payload []byte) uint32 {
	if len(payload) == 0 {
		return TextPlain
	}

	// Try JSON
	var jsonData interface{}
	if json.Unmarshal(payload, &jsonData) == nil {
		return AppJSON
	}

	// Try XML (simple check)
	trimmed := bytes.TrimSpace(payload)
	if bytes.HasPrefix(trimmed, []byte("<")) && bytes.HasSuffix(trimmed, []byte(">")) {
		return AppXML
	}

	// Check if it's printable text
	for _, b := range payload {
		if b < 32 && b != 9 && b != 10 && b != 13 { // Allow tab, LF, CR
			return AppOctets
		}
		if b > 126 {
			return AppOctets
		}
	}

	return TextPlain
}

func (c *Converter) contentFormatToMimeType(cf uint32) string {
	switch cf {
	case TextPlain:
		return "text/plain"
	case AppLinkFormat:
		return "application/link-format"
	case AppXML:
		return "application/xml"
	case AppOctets:
		return "application/octet-stream"
	case AppJSON:
		return "application/json"
	case AppCBOR:
		return "application/cbor"
	default:
		return "application/octet-stream"
	}
}

func (c *Converter) mimeTypeToContentFormat(mimeType string) uint32 {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "text/plain":
		return TextPlain
	case "application/link-format":
		return AppLinkFormat
	case "application/xml", "text/xml":
		return AppXML
	case "application/json":
		return AppJSON
	case "application/cbor":
		return AppCBOR
	default:
		return AppOctets
	}
}

func (c *Converter) addOptionsFromMetadata(coapMsg *message.Message, msg *service.Message) {
	// Note: go-coap v3 requires buffer-based option setting which is complex
	// For now, we'll implement a simplified version using the higher-level methods

	// URI Path - use SetPath if available
	if uriPath, exists := msg.MetaGet("coap_uri_path"); exists {
		path := strings.Trim(uriPath, "/")
		if path != "" {
			// In go-coap v3, we would need to use Options.SetPath with a buffer
			// For now, we'll skip complex option setting
			c.logger.Debug(fmt.Sprintf("Would set URI path: %s", path))
		}
	}

	// For other options, we would need to implement buffer-based option setting
	// This is complex in go-coap v3 and requires careful buffer management
	// TODO: Implement proper option setting with buffers

	// Log metadata for debugging
	if uriQuery, exists := msg.MetaGet("coap_uri_query"); exists {
		c.logger.Debug(fmt.Sprintf("Would set URI query: %s", uriQuery))
	}
	if maxAge, exists := msg.MetaGet("coap_max_age"); exists {
		c.logger.Debug(fmt.Sprintf("Would set max age: %s", maxAge))
	}
}

func (c *Converter) preserveAllOptions(msg *service.Message, coapMsg *message.Message) {
	options := make(map[string]interface{})

	// Note: In go-coap v3, Options is a slice, not something that supports Visit()
	// We'll need a different approach to preserve all options
	// For now, we'll skip this functionality
	// TODO: Implement option preservation for go-coap v3
	_ = options // Avoid unused variable warning
}

func (c *Converter) getOptionName(optionID message.OptionID) string {
	switch optionID {
	case message.IfMatch:
		return "if_match"
	case message.URIHost:
		return "uri_host"
	case message.ETag:
		return "etag"
	case message.IfNoneMatch:
		return "if_none_match"
	case message.Observe:
		return "observe"
	case message.URIPort:
		return "uri_port"
	case message.LocationPath:
		return "location_path"
	case message.URIPath:
		return "uri_path"
	case message.ContentFormat:
		return "content_format"
	case message.MaxAge:
		return "max_age"
	case message.URIQuery:
		return "uri_query"
	case message.Accept:
		return "accept"
	case message.LocationQuery:
		return "location_query"
	case message.Block2:
		return "block2"
	case message.Block1:
		return "block1"
	case message.Size2:
		return "size2"
	case message.ProxyURI:
		return "proxy_uri"
	case message.ProxyScheme:
		return "proxy_scheme"
	case message.Size1:
		return "size1"
	default:
		return fmt.Sprintf("option_%d", int(optionID))
	}
}

func (c *Converter) compressPayload(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)

	if _, err := gzWriter.Write(payload); err != nil {
		return nil, err
	}

	if err := gzWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *Converter) decompressGzip(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (c *Converter) decompressDeflate(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("deflate payload is empty")
	}

	reader := bytes.NewReader(payload)
	zlibReader, err := zlib.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create zlib reader: %w", err)
	}
	defer zlibReader.Close()

	decompressed, err := io.ReadAll(zlibReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}
	return decompressed, nil
}
