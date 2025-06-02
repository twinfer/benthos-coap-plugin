package converter

import (
	"bytes"
	"compress/gzip" // Added for GzippedData test
	"compress/zlib"
	"io"
	"strings"
	"testing"

	"github.com/plgd-dev/go-coap/v3/message" // Required for constructing CoAP messages
	"github.com/redpanda-data/benthos/v4/public/service"
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
