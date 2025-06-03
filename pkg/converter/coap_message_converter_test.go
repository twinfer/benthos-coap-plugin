package converter

import (
	"bytes"
	"compress/gzip" // Added for GzippedData test
	"compress/zlib"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to compress data with zlib (deflate)
func compressWithZlib(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func TestDecompressDeflate(t *testing.T) {
	// Create a dummy converter instance (not strictly needed for decompressDeflate as it's a method on *Converter,
	// but good practice if it were to access c.logger or c.config in the future)
	// For now, decompressDeflate doesn't use any fields from Converter, so a nil logger is fine.
	// If it did, we'd initialize logger and config properly.
	c := &Converter{
		logger: service.MockLogger(), // Using a mock logger
		config: Config{},             // Default config
	}

	t.Run("ValidDeflatePayload", func(t *testing.T) {
		originalPayload := "Hello, World! This is a test string for deflate compression."
		originalBytes := []byte(originalPayload)

		compressedBytes, err := compressWithZlib(originalBytes)
		if err != nil {
			t.Fatalf("Failed to compress data for test: %v", err)
		}

		decompressedBytes, err := c.decompressDeflate(compressedBytes)
		if err != nil {
			t.Errorf("decompressDeflate() error = %v, wantErr %v", err, false)
			return
		}
		if string(decompressedBytes) != originalPayload {
			t.Errorf("decompressDeflate() got = %s, want %s", string(decompressedBytes), originalPayload)
		}
	})

	t.Run("InvalidDeflatePayload_NotZlib", func(t *testing.T) {
		invalidPayload := []byte("This is not a valid deflate stream.")
		_, err := c.decompressDeflate(invalidPayload)
		if err == nil {
			t.Errorf("decompressDeflate() expected an error for non-zlib payload, but got nil")
		} else {
			// Check for specific zlib errors if possible, or a general error message
			// zlib.ErrChecksum or zlib.ErrHeader are common
			if !strings.Contains(err.Error(), zlib.ErrChecksum.Error()) && !strings.Contains(err.Error(), zlib.ErrHeader.Error()) && !strings.Contains(err.Error(), "invalid header") {
				t.Logf("Received error: %v. This might be acceptable if it indicates a zlib format error.", err)
			}
		}
	})

	t.Run("InvalidDeflatePayload_GzippedData", func(t *testing.T) {
		// Gzip data is different from deflate
		var b bytes.Buffer
		gzWriterReal := gzip.NewWriter(&b) // Use real gzip writer
		if _, err := gzWriterReal.Write([]byte("gzipped data")); err != nil {
			t.Fatalf("Failed to create gzip data: %v", err)
		}
		gzWriterReal.Close()
		invalidPayload := b.Bytes()

		_, err := c.decompressDeflate(invalidPayload)
		if err == nil {
			t.Errorf("decompressDeflate() expected an error for gzipped payload, but got nil")
		} else {
			if !strings.Contains(err.Error(), zlib.ErrChecksum.Error()) && !strings.Contains(err.Error(), zlib.ErrHeader.Error()) && !strings.Contains(err.Error(), "invalid header") {
				t.Logf("Received error for gzipped data: %v. This might be acceptable if it indicates a zlib format error.", err)
			}
		}
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		emptyPayload := []byte{}
		_, err := c.decompressDeflate(emptyPayload)
		if err == nil {
			t.Errorf("decompressDeflate() expected an error for empty payload, but got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "payload is empty") {
			t.Errorf("decompressDeflate() error = %v, want err containing 'payload is empty'", err)
		}
	})

	t.Run("ValidDeflatePayload_Short", func(t *testing.T) {
		originalPayload := "short"
		originalBytes := []byte(originalPayload)

		compressedBytes, err := compressWithZlib(originalBytes)
		if err != nil {
			t.Fatalf("Failed to compress data for test: %v", err)
		}

		decompressedBytes, err := c.decompressDeflate(compressedBytes)
		if err != nil {
			t.Errorf("decompressDeflate() error = %v, wantErr %v", err, false)
			return
		}
		if string(decompressedBytes) != originalPayload {
			t.Errorf("decompressDeflate() got = %s, want %s", string(decompressedBytes), originalPayload)
		}
	})

	t.Run("CorruptedDeflatePayload_BadChecksum", func(t *testing.T) {
		originalPayload := "Valid payload then corrupted"
		compressedBytes, err := compressWithZlib([]byte(originalPayload))
		if err != nil {
			t.Fatalf("Failed to compress data: %v", err)
		}

		// Corrupt the payload (e.g., flip a bit in the checksum or data)
		// A simple way is to truncate it or alter last few bytes
		if len(compressedBytes) > 2 {
			compressedBytes[len(compressedBytes)-1]++ // Modify last byte
		} else {
			t.Skip("Compressed payload too short to corrupt for this test")
		}

		_, err = c.decompressDeflate(compressedBytes)
		if err == nil {
			t.Errorf("decompressDeflate() expected an error for corrupted payload, but got nil")
		} else if !strings.Contains(err.Error(), zlib.ErrChecksum.Error()) && !strings.Contains(err.Error(), io.ErrUnexpectedEOF.Error()) {
			// Depending on corruption, could be checksum or unexpected EOF if zlib processes some data first
			t.Logf("Received error for corrupted data: %v. Expected checksum error or unexpected EOF.", err)
		}
	})
}

func TestMessageToCoAP_ContentFormat(t *testing.T) {
	logger := service.MockLogger()
	tests := []struct {
		name                 string
		config               Config
		benthosMsg           *service.Message
		expectedContentFormat uint32
		expectError          bool
	}{
		{
			name: "from coap_content_format metadata",
			config: Config{},
			benthosMsg: func() *service.Message {
				msg := service.NewMessage([]byte("hello"))
				msg.MetaSet("coap_content_format", "50") // application/json
				return msg
			}(),
			expectedContentFormat: message.AppJSON,
		},
		{
			name: "from content_type metadata",
			config: Config{},
			benthosMsg: func() *service.Message {
				msg := service.NewMessage([]byte("hello"))
				msg.MetaSet("content_type", "application/xml")
				return msg
			}(),
			expectedContentFormat: message.AppXML,
		},
		{
			name: "auto-detect JSON",
			config: Config{},
			benthosMsg: service.NewMessage([]byte(`{"key":"value"}`)),
			expectedContentFormat: message.AppJSON,
		},
		{
			name: "auto-detect XML",
			config: Config{},
			benthosMsg: service.NewMessage([]byte(`<tag>value</tag>`)),
			expectedContentFormat: message.AppXML,
		},
		{
			name: "auto-detect plain text",
			config: Config{},
			benthosMsg: service.NewMessage([]byte(`hello plain text`)),
			expectedContentFormat: message.TextPlain,
		},
		{
			name: "default to AppOctets for non-textual",
			config: Config{},
			benthosMsg: service.NewMessage([]byte{0x01, 0x02, 0x03, 0xFF}), // some binary
			expectedContentFormat: message.AppOctets,
		},
		{
			name: "empty payload results in text plain",
			config: Config{},
			benthosMsg: service.NewMessage([]byte{}),
			expectedContentFormat: message.TextPlain,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := NewConverter(tt.config, logger)
			coapMsg, err := conv.MessageToCoAP(tt.benthosMsg)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, coapMsg)

			cf, err := coapMsg.Options().GetUint32(message.ContentFormat)
			require.NoError(t, err, "ContentFormat option should be present")
			assert.Equal(t, tt.expectedContentFormat, cf)
		})
	}
}

func TestCoAPMessageOptionPreservation(t *testing.T) {
	logger := service.MockLogger()
	converter := NewConverter(Config{PreserveOptions: true}, logger)

	t.Run("CoAPToMessage then MessageToCoAP", func(t *testing.T) {
		// 1. CoAPToMessage (Preserve CoAP options to Benthos metadata)
		coapIn := pool.AcquireMessage(message.TODO)
		defer pool.ReleaseMessage(coapIn)
		coapIn.SetCode(codes.Content)
		coapIn.SetToken(message.Token("token1"))
		coapIn.SetPayload([]byte("payload"))

		// Add options that should be preserved
		require.NoError(t, coapIn.AddOptionBytes(message.OptionID(2000), []byte("custom_opt_val_1"))) // Custom option
		require.NoError(t, coapIn.AddOptionString(message.OptionID(2001), "custom_opt_val_2_str"))
		require.NoError(t, coapIn.AddOptionBytes(message.OptionID(2000), []byte("custom_opt_val_1_repeated"))) // Repeated custom option

		// Add options that should be skipped by generic preservation (handled by specific metadata)
		require.NoError(t, coapIn.SetOptionUint32(message.MaxAge, 60))
		require.NoError(t, coapIn.SetPathString("/skipped/path"))


		benthosMsg, err := converter.CoAPToMessage(coapIn)
		require.NoError(t, err)
		require.NotNil(t, benthosMsg)

		// Verify specific metadata
		v, exists := benthosMsg.MetaGet("coap_max_age")
		assert.True(t, exists)
		assert.Equal(t, "60", v)
		v, exists = benthosMsg.MetaGet("coap_uri_path")
		assert.True(t, exists)
		assert.Equal(t, "/skipped/path", v)


		// Verify preserved generic options
		expectedMetaKey1 := "coap_option_2000_0"
		expectedMetaVal1 := hex.EncodeToString([]byte("custom_opt_val_1"))
		v, exists = benthosMsg.MetaGet(expectedMetaKey1)
		assert.True(t, exists, "Expected metadata key %s", expectedMetaKey1)
		assert.Equal(t, expectedMetaVal1, v)

		expectedMetaKey2 := "coap_option_2001_0"
		expectedMetaVal2 := hex.EncodeToString([]byte("custom_opt_val_2_str"))
		v, exists = benthosMsg.MetaGet(expectedMetaKey2)
		assert.True(t, exists, "Expected metadata key %s", expectedMetaKey2)
		assert.Equal(t, expectedMetaVal2, v)

		expectedMetaKey3 := "coap_option_2000_1" // Second occurrence of option 2000
		expectedMetaVal3 := hex.EncodeToString([]byte("custom_opt_val_1_repeated"))
		v, exists = benthosMsg.MetaGet(expectedMetaKey3)
		assert.True(t, exists, "Expected metadata key %s for repeated option", expectedMetaKey3)
		assert.Equal(t, expectedMetaVal3, v)


		// Verify skipped options are not in generic metadata
		_, exists = benthosMsg.MetaGet("coap_option_14_0") // MaxAge (14)
		assert.False(t, exists, "MaxAge should not be in generic preserved options")
		_, exists = benthosMsg.MetaGet("coap_option_11_0") // UriPath (11)
		assert.False(t, exists, "UriPath should not be in generic preserved options")

		// 2. MessageToCoAP (Restore options from Benthos metadata to CoAP message)
		// We need to remove the specific metadata that preserveAllOptions would skip,
		// so that addOptionsFromMetadata relies on the generic preserved ones for these.
		// No, this is not correct. `addOptionsFromMetadata` will use specific keys if present,
		// then generic ones. `preserveAllOptions` already skips specific ones.
		// So the benthosMsg is fine as is.

		coapOut, err := converter.MessageToCoAP(benthosMsg)
		require.NoError(t, err)
		require.NotNil(t, coapOut)

		// Verify restored options
		opts := coapOut.Options()
		var restoredOpt2000Values [][]byte
		var restoredOpt2001Values []string
		var restoredMaxAge uint32
		var restoredPath string

		restoredPath, _ = coapOut.Options().Path()
		restoredMaxAge, _ = coapOut.Options().GetUint32(message.MaxAge)


		for _, opt := range opts {
			if opt.ID == message.OptionID(2000) {
				restoredOpt2000Values = append(restoredOpt2000Values, opt.Value)
			}
			if opt.ID == message.OptionID(2001) {
				restoredOpt2001Values = append(restoredOpt2001Values, string(opt.Value))
			}
		}

		assert.Equal(t, "/skipped/path", restoredPath, "Path from specific metadata should be restored")
		assert.Equal(t, uint32(60), restoredMaxAge, "MaxAge from specific metadata should be restored")

		require.Len(t, restoredOpt2000Values, 2, "Should have two instances of option ID 2000")
		assert.Contains(t, restoredOpt2000Values, []byte("custom_opt_val_1"))
		assert.Contains(t, restoredOpt2000Values, []byte("custom_opt_val_1_repeated"))

		require.Len(t, restoredOpt2001Values, 1, "Should have one instance of option ID 2001")
		assert.Contains(t, restoredOpt2001Values, "custom_opt_val_2_str")
	})
}


func TestMessageToCoAP_SpecificOptionSetting(t *testing.T) {
	logger := service.MockLogger()
	// Test with PreserveOptions = false to isolate specific logic
	converter := NewConverter(Config{PreserveOptions: false}, logger)

	tests := []struct {
		name          string
		meta          map[string]string
		verifyOpt     func(t *testing.T, opts message.Options)
		expectedError bool
	}{
		{
			name: "set Uri-Path",
			meta: map[string]string{"coap_uri_path": "/sensors/temp"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				path, err := opts.Path()
				require.NoError(t, err)
				assert.Equal(t, "sensors/temp", path)
			},
		},
		{
			name: "set Uri-Query",
			meta: map[string]string{"coap_uri_query": "rt=temperature&unit=celsius"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				queries, err := opts.Queries()
				require.NoError(t, err)
				assert.Contains(t, queries, "rt=temperature")
				assert.Contains(t, queries, "unit=celsius")
			},
		},
		{
			name: "set Max-Age",
			meta: map[string]string{"coap_max_age": "120"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				val, err := opts.GetUint32(message.MaxAge)
				require.NoError(t, err)
				assert.Equal(t, uint32(120), val)
			},
		},
		{
			name: "set ETag",
			meta: map[string]string{"coap_etag": "simpleTag"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				val, err := opts.GetBytes(message.ETag)
				require.NoError(t, err)
				assert.Equal(t, []byte("simpleTag"), val)
			},
		},
		{
			name: "set Observe register",
			meta: map[string]string{"coap_observe_request": "register"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				val, err := opts.GetUint32(message.Observe)
				require.NoError(t, err)
				assert.Equal(t, uint32(0), val)
			},
		},
		{
			name: "set Observe deregister",
			meta: map[string]string{"coap_observe_request": "1"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				val, err := opts.GetUint32(message.Observe)
				require.NoError(t, err)
				assert.Equal(t, uint32(1), val)
			},
		},
		{
			name: "set Accept",
			meta: map[string]string{"coap_accept": "50"}, // application/json
			verifyOpt: func(t *testing.T, opts message.Options) {
				val, err := opts.GetUint32(message.Accept)
				require.NoError(t, err)
				assert.Equal(t, uint32(50), val)
			},
		},
		{
			name: "set If-Match (single hex string)",
			meta: map[string]string{"coap_if_match": hex.EncodeToString([]byte{0x01, 0x02})},
			verifyOpt: func(t *testing.T, opts message.Options) {
				// GetOptionBytes returns first if multiple, GetOptions returns slice
				vals, err := opts.GetOptionBytes(message.IfMatch) // Returns first if multiple
				require.NoError(t, err)
				assert.Equal(t, []byte{0x01, 0x02}, vals)
			},
		},
		{
			name: "set If-None-Match",
			meta: map[string]string{"coap_if_none_match": "true"},
			verifyOpt: func(t *testing.T, opts message.Options) {
				_, err := opts.GetOptionBytes(message.IfNoneMatch) // Check for presence
				require.NoError(t, err) // If present, err is nil, value is empty []byte
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bMsg := service.NewMessage(nil)
			for k, v := range tt.meta {
				bMsg.MetaSet(k, v)
			}

			coapMsg, err := converter.MessageToCoAP(bMsg)
			if tt.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, coapMsg)
			tt.verifyOpt(t, coapMsg.Options())
		})
	}
}


func TestMessageToCoAP_SpecificOptionPrecedence(t *testing.T) {
	logger := service.MockLogger()
	// Test with PreserveOptions = true to check precedence
	converter := NewConverter(Config{PreserveOptions: true}, logger)

	bMsg := service.NewMessage(nil)
	// Specific metadata for ETag
	bMsg.MetaSet("coap_etag", "specific-etag-value")
	// Generic preserved option for ETag (ID 4)
	bMsg.MetaSet("coap_option_4_0", hex.EncodeToString([]byte("generic-etag-value")))

	coapMsg, err := converter.MessageToCoAP(bMsg)
	require.NoError(t, err)
	require.NotNil(t, coapMsg)

	// Verify that the specific ETag is used
	etagBytes, err := coapMsg.Options().GetBytes(message.ETag)
	require.NoError(t, err)
	assert.Equal(t, []byte("specific-etag-value"), etagBytes)

	// Verify that the generic one was NOT added additionally if SetOptionBytes was used for specific.
	// If AddOptionBytes was used, there might be two. ETag is usually single.
	// Our specific handler uses SetOptionBytes.
	// The generic restore logic has a safeguard to skip ID 4 if coap_option_4_0 is encountered.

	// Let's check how many ETag options are there. Should be 1.
	count := 0
	for _, opt := range coapMsg.Options() {
		if opt.ID == message.ETag {
			count++
		}
	}
	assert.Equal(t, 1, count, "Only one ETag option should be present, set by specific metadata")
}

// Test for MessageToCoAP to ensure correct URI path and query formation
// when PreserveOptions is false (generic options are not processed).
func TestMessageToCoAP_PathAndQuery_NoPreserve(t *testing.T) {
	logger := service.MockLogger()
	converterNoPreserve := NewConverter(Config{PreserveOptions: false}, logger)

	bMsg := service.NewMessage([]byte("test payload"))
	bMsg.MetaSet("coap_uri_path", "/my/path")
	bMsg.MetaSet("coap_uri_query", "q1=v1&q2=v2")
	// Add a generic option that should be ignored
	bMsg.MetaSet("coap_option_100_0", "ignored")


	coapMsg, err := converterNoPreserve.MessageToCoAP(bMsg)
	require.NoError(t, err)
	require.NotNil(t, coapMsg)

	path, _ := coapMsg.Options().Path()
	assert.Equal(t, "my/path", path)

	queries, _ := coapMsg.Options().Queries()
	assert.ElementsMatch(t, []string{"q1=v1", "q2=v2"}, queries)

	// Check that the generic option was ignored
	_, err = coapMsg.Options().GetOptionBytes(message.OptionID(100))
	assert.Error(t, err, "Generic option should not be present when PreserveOptions is false")
}

// Helper function to compress data with gzip
func compressWithGzip(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func TestExtractPayload(t *testing.T) {
	c := &Converter{
		logger: service.MockLogger(),
		config: Config{}, // Use default config for these tests
	}

	t.Run("DeflateEncodedPayload", func(t *testing.T) {
		originalData := "hello deflate this is a test"
		compressedData, err := compressWithZlib([]byte(originalData))
		if err != nil {
			t.Fatalf("Failed to compress with zlib: %v", err)
		}

		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload(compressedData)
		coapMsg.SetOptionString(message.ContentEncoding, "deflate")
		defer message.ReleaseMessageCtx(coapMsg.Context()) // Important to release context

		extracted, err := c.extractPayload(coapMsg)
		if err != nil {
			t.Errorf("extractPayload() error = %v, wantErr false", err)
			return
		}
		if string(extracted) != originalData {
			t.Errorf("extractPayload() got = %s, want %s", string(extracted), originalData)
		}
	})

	t.Run("DeflateEncodedPayload_Uppercase", func(t *testing.T) {
		originalData := "hello DEFLATE this is a test with uppercase encoding"
		compressedData, err := compressWithZlib([]byte(originalData))
		if err != nil {
			t.Fatalf("Failed to compress with zlib: %v", err)
		}

		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload(compressedData)
		coapMsg.SetOptionString(message.ContentEncoding, "DEFLATE") // Uppercase
		defer message.ReleaseMessageCtx(coapMsg.Context())

		extracted, err := c.extractPayload(coapMsg)
		if err != nil {
			t.Errorf("extractPayload() error = %v, wantErr false for uppercase encoding", err)
			return
		}
		if string(extracted) != originalData {
			t.Errorf("extractPayload() got = %s, want %s for uppercase encoding", string(extracted), originalData)
		}
	})

	t.Run("GzipEncodedPayload", func(t *testing.T) {
		originalData := "hello gzip this is a test"
		compressedData, err := compressWithGzip([]byte(originalData))
		if err != nil {
			t.Fatalf("Failed to compress with gzip: %v", err)
		}

		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload(compressedData)
		coapMsg.SetOptionString(message.ContentEncoding, "gzip")
		defer message.ReleaseMessageCtx(coapMsg.Context())

		extracted, err := c.extractPayload(coapMsg)
		if err != nil {
			t.Errorf("extractPayload() error = %v, wantErr false for gzip", err)
			return
		}
		if string(extracted) != originalData {
			t.Errorf("extractPayload() got = %s, want %s for gzip", string(extracted), originalData)
		}
	})

	t.Run("NoEncoding", func(t *testing.T) {
		originalData := "hello plain no encoding"
		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload([]byte(originalData))
		// No Content-Encoding option set
		defer message.ReleaseMessageCtx(coapMsg.Context())

		extracted, err := c.extractPayload(coapMsg)
		if err != nil {
			t.Errorf("extractPayload() error = %v, wantErr false for no encoding", err)
			return
		}
		if string(extracted) != originalData {
			t.Errorf("extractPayload() got = %s, want %s for no encoding", string(extracted), originalData)
		}
	})

	t.Run("UnknownEncoding", func(t *testing.T) {
		originalData := "hello unknown encoding"
		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload([]byte(originalData))
		coapMsg.SetOptionString(message.ContentEncoding, "br") // Brotli, or any other unknown
		defer message.ReleaseMessageCtx(coapMsg.Context())

		extracted, err := c.extractPayload(coapMsg)
		if err != nil {
			t.Errorf("extractPayload() error = %v, wantErr false for unknown encoding", err)
			return
		}
		// Expect original data as extractPayload should pass it through
		if string(extracted) != originalData {
			t.Errorf("extractPayload() got = %s, want %s for unknown encoding", string(extracted), originalData)
		}
	})

	t.Run("DeflateEncodedEmptyPayload", func(t *testing.T) {
		// This test is tricky. An empty payload, when "deflate" encoded,
		// should ideally result in an error from decompressDeflate.
		// The CoAP message itself can have an empty payload.
		// The current decompressDeflate returns an error for empty input.
		compressedData := []byte{} // Empty compressed data

		coapMsg := message.NewMessage(message.AcquireMessageCtx())
		coapMsg.SetPayload(compressedData) // An empty payload that claims to be deflate
		coapMsg.SetOptionString(message.ContentEncoding, "deflate")
		defer message.ReleaseMessageCtx(coapMsg.Context())

		_, err := c.extractPayload(coapMsg)
		if err == nil {
			t.Errorf("extractPayload() expected error for deflate encoded empty payload, got nil")
		} else if !strings.Contains(err.Error(), "payload is empty") {
			// This check depends on the error message from decompressDeflate
			t.Errorf("extractPayload() error = %v, want error containing 'payload is empty'", err)
		}
	})
}
