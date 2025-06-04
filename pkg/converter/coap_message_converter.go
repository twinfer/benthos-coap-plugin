// pkg/converter/converter.go
package converter

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
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

	// Create a new CoAP message
	coapMsg := &message.Message{
		Type: message.Confirmable, // Default to Confirmable
	}

	// Set method from metadata or default to POST
	method := c.getMethodFromMetadata(msg)
	coapMsg.Code = method

	// Set content format
	contentFormatValue, err := c.determineContentFormat(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to determine content format: %w", err)
	}

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

	// Set the ContentFormat option
	buf := make([]byte, 32) // Allocate buffer for option encoding
	var err2 error
	coapMsg.Options, _, err2 = coapMsg.Options.SetContentFormat(buf, message.MediaType(contentFormatValue))
	if err2 != nil {
		c.logger.Warnf("Failed to set CoAP ContentFormat option: %v", err2)
	}

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
	// Handle explicitly set URI path
	if uriPath, exists := msg.MetaGet("coap_uri_path"); exists {
		var err error
		pathBuf := make([]byte, 256) // Fresh buffer for path
		coapMsg.Options, _, err = coapMsg.Options.SetPath(pathBuf, strings.TrimPrefix(uriPath, "/"))
		if err != nil {
			c.logger.Warnf("Failed to set CoAP URI Path from 'coap_uri_path' metadata: %v", err)
		}
	}

	// Handle explicitly set URI query
	if uriQueryStr, exists := msg.MetaGet("coap_uri_query"); exists {
		queries := strings.Split(uriQueryStr, "&")
		for _, q := range queries {
			var err error
			queryBuf := make([]byte, 256) // Fresh buffer for each query
			coapMsg.Options, _, err = coapMsg.Options.AddString(queryBuf, message.URIQuery, q)
			if err != nil {
				c.logger.Warnf("Failed to add URI query '%s': %v", q, err)
			}
		}
	}

	// Max-Age
	if maxAgeStr, exists := msg.MetaGet("coap_max_age"); exists {
		if maxAgeVal, err := strconv.ParseUint(maxAgeStr, 10, 32); err == nil {
			var err2 error
			maxAgeBuf := make([]byte, 256)
			coapMsg.Options, _, err2 = coapMsg.Options.SetUint32(maxAgeBuf, message.MaxAge, uint32(maxAgeVal))
			if err2 != nil {
				c.logger.Warnf("Failed to set CoAP Max-Age option from metadata: %v", err2)
			}
		} else {
			c.logger.Warnf("Failed to parse 'coap_max_age' metadata value '%s' as uint32: %v", maxAgeStr, err)
		}
	}

	// ETag
	if etagStr, exists := msg.MetaGet("coap_etag"); exists {
		// Assuming ETag in metadata is a direct string. If it can be hex, add decoding.
		var err error
		etagBuf := make([]byte, 256)
		coapMsg.Options, _, err = coapMsg.Options.SetBytes(etagBuf, message.ETag, []byte(etagStr))
		if err != nil {
			c.logger.Warnf("Failed to set CoAP ETag option from metadata: %v", err)
		}
	}

	// Observe
	if obsReqStr, exists := msg.MetaGet("coap_observe_request"); exists {
		val := strings.ToLower(obsReqStr)
		var obsVal uint32
		setObs := true
		if val == "register" || val == "0" {
			obsVal = 0 // Register
		} else if val == "deregister" || val == "1" {
			obsVal = 1 // Deregister
		} else {
			setObs = false
			c.logger.Warnf("Invalid value for 'coap_observe_request': %s. Expected 'register', 'deregister', '0', or '1'.", obsReqStr)
		}
		if setObs {
			var err error
			observeBuf := make([]byte, 256)
			coapMsg.Options, _, err = coapMsg.Options.SetUint32(observeBuf, message.Observe, obsVal)
			if err != nil {
				c.logger.Warnf("Failed to set CoAP Observe option from metadata: %v", err)
			}
		}
	}

	// Accept
	if acceptStr, exists := msg.MetaGet("coap_accept"); exists {
		if acceptVal, err := strconv.ParseUint(acceptStr, 10, 16); err == nil {
			var err2 error
			acceptBuf := make([]byte, 256)
			coapMsg.Options, _, err2 = coapMsg.Options.SetAccept(acceptBuf, message.MediaType(acceptVal))
			if err2 != nil {
				c.logger.Warnf("Failed to set CoAP Accept option from metadata: %v", err2)
			}
		} else {
			c.logger.Warnf("Failed to parse 'coap_accept' metadata value '%s' as uint16/uint32: %v", acceptStr, err)
		}
	}

	// If-Match
	if ifMatchStr, exists := msg.MetaGet("coap_if_match"); exists {
		// Assuming If-Match in metadata is a hex-encoded string.
		// CoAP spec allows multiple If-Match options.
		decodedVal, err := hex.DecodeString(ifMatchStr)
		if err == nil {
			var err2 error
			ifMatchBuf := make([]byte, 256)
			coapMsg.Options, _, err2 = coapMsg.Options.AddBytes(ifMatchBuf, message.IfMatch, decodedVal)
			if err2 != nil {
				c.logger.Warnf("Failed to set CoAP If-Match option from metadata: %v", err2)
			}
		} else {
			c.logger.Warnf("Failed to hex-decode 'coap_if_match' metadata value '%s': %v", ifMatchStr, err)
		}
	}
	// To support multiple If-Match values from metadata, one might use "coap_if_match_0", "coap_if_match_1"
	// or a JSON array string. For simplicity, current handles one hex string.

	// If-None-Match
	if _, exists := msg.MetaGet("coap_if_none_match"); exists {
		// The presence of the key indicates the option should be set. It's a zero-byte option.
		var err error
		ifNoneMatchBuf := make([]byte, 256)
		coapMsg.Options, _, err = coapMsg.Options.SetBytes(ifNoneMatchBuf, message.IfNoneMatch, []byte{})
		if err != nil {
			c.logger.Warnf("Failed to set CoAP If-None-Match option from metadata: %v", err)
		}
	}

	// Logic for restoring options preserved by preserveAllOptions (if enabled)
	// This runs after specific options are handled, allowing specific metadata to take precedence
	// if an option (like ETag) was also somehow in generic preserved options.
	// However, preserveAllOptions is designed to skip these specific IDs, so direct conflict is unlikely.
	if c.config.PreserveOptions {
		type preservedOption struct {
			ID    message.OptionID
			Index int
			Value []byte // Raw byte value after hex decoding
		}
		var optionsToRestore []preservedOption

		msg.MetaWalk(func(k string, v string) error {
			if strings.HasPrefix(k, "coap_option_") {
				var optID uint16
				var optIndex int
				// Example key: coap_option_270_0 (Option ID 270, index 0)
				n, err := fmt.Sscanf(k, "coap_option_%d_%d", &optID, &optIndex)
				if err == nil && n == 2 {
					// Ensure this option ID is not one handled by specific metadata fields above,
					// to avoid adding it twice if it somehow slipped through preserveAllOptions' skip logic
					// or if user manually set both coap_etag and coap_option_4_0.
					// The specific handlers above should take precedence.
					switch message.OptionID(optID) {
					case message.URIPath, message.URIQuery, message.ContentFormat, message.MaxAge, message.ETag, message.Observe, message.Accept, message.IfMatch, message.IfNoneMatch:
						// These are handled by specific metadata keys above.
						// If they are found here, it means preserveAllOptions might not have skipped them,
						// or user set them manually. We prefer the specific handler's value.
						// However, preserveAllOptions IS designed to skip these.
						// This check is an extra safeguard.
						c.logger.Debugf("Skipping restore of generic coap_option_%d_%d as it's handled by specific metadata.", optID, optIndex)
						return nil
					}

					decodedVal, decodeErr := hex.DecodeString(v)
					if decodeErr != nil {
						c.logger.Warnf("Failed to hex-decode metadata value for key %s: %v", k, decodeErr)
						return nil
					}
					optionsToRestore = append(optionsToRestore, preservedOption{
						ID:    message.OptionID(optID),
						Index: optIndex,
						Value: decodedVal,
					})
				}
			}
			return nil
		})

		// Sort options by ID and then by Index to ensure correct order of addition
		sort.Slice(optionsToRestore, func(i, j int) bool {
			if optionsToRestore[i].ID != optionsToRestore[j].ID {
				return optionsToRestore[i].ID < optionsToRestore[j].ID
			}
			return optionsToRestore[i].Index < optionsToRestore[j].Index
		})

		for _, opt := range optionsToRestore {
			var err error
			restoreBuf := make([]byte, 256)
			coapMsg.Options, _, err = coapMsg.Options.AddBytes(restoreBuf, opt.ID, opt.Value)
			if err != nil {
				c.logger.Warnf("Failed to restore CoAP option ID %d (Index %d) from metadata: %v", opt.ID, opt.Index, err)
			} else {
				c.logger.Debugf("Restored generic CoAP option ID %d (Index %d) from metadata", opt.ID, opt.Index)
			}
		}
	}
}

func (c *Converter) preserveAllOptions(msg *service.Message, coapMsg *message.Message) {
	if !c.config.PreserveOptions {
		return
	}

	// Keep track of how many times we've seen an option ID to create unique metadata keys
	optionCounts := make(map[message.OptionID]int)

	for _, opt := range coapMsg.Options {
		// Skip options that are typically handled by specific fields/logic,
		// to avoid redundancy or conflict if they are also restored generically.
		// Users can get these from their specific metadata fields like coap_uri_path, coap_content_format.
		switch opt.ID {
		case message.URIPath, message.URIQuery, message.ContentFormat, message.LocationPath, message.Observe, message.MaxAge, message.ETag:
			// These are already explicitly extracted in addCoAPMetadata or handled elsewhere.
			// If we also save them here, during restoration (Part 2) we might add them twice
			// or cause conflicts if the specific metadata (e.g. "coap_uri_path") was modified.
			// So, we skip them from generic preservation.
			continue
		}

		baseKey := fmt.Sprintf("coap_option_%d", opt.ID)
		occurrence := optionCounts[opt.ID]
		optionCounts[opt.ID]++ // Increment for next time

		// Create a unique key for each occurrence of an option
		metaKey := fmt.Sprintf("%s_%d", baseKey, occurrence)
		metaValue := hex.EncodeToString(opt.Value)

		msg.MetaSet(metaKey, metaValue)
		c.logger.Debugf("Preserved CoAP option %s (ID: %d, Occurrence: %d) as metadata key '%s'", c.getOptionName(opt.ID), opt.ID, occurrence, metaKey)
	}
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
