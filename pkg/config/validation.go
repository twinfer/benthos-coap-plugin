// pkg/config/validation.go
package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var (
	// Valid CoAP protocols
	validProtocols = map[string]bool{
		"udp":      true,
		"tcp":      true,
		"udp-dtls": true,
		"tcp-tls":  true,
	}

	// Valid security modes per protocol
	validSecurityModes = map[string][]string{
		"udp":      {"none"},
		"tcp":      {"none"},
		"udp-dtls": {"psk", "certificate"},
		"tcp-tls":  {"certificate", "none"},
	}

	// CoAP path pattern (allows wildcards)
	pathPattern = regexp.MustCompile(`^/[a-zA-Z0-9\-_.~+/]*(\+|[a-zA-Z0-9\-_.~])*$`)
)

// ValidateProtocol checks if the protocol is supported
func ValidateProtocol(protocol string) error {
	if !validProtocols[protocol] {
		return fmt.Errorf("unsupported protocol %s, must be one of: %s",
			protocol, strings.Join(getKeys(validProtocols), ", "))
	}
	return nil
}

// ValidateEndpoint validates a CoAP endpoint URL
func ValidateEndpoint(endpoint, protocol string) error {
	parsedURL, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	// Validate scheme matches protocol
	expectedScheme := getExpectedScheme(protocol)
	if parsedURL.Scheme != expectedScheme {
		return fmt.Errorf("scheme %s does not match protocol %s (expected %s)",
			parsedURL.Scheme, protocol, expectedScheme)
	}

	// Validate host is present
	if parsedURL.Host == "" {
		return fmt.Errorf("host is required")
	}

	// Validate port (if specified)
	if parsedURL.Port() != "" {
		if parsedURL.Port() == "0" {
			return fmt.Errorf("port cannot be 0")
		}
	}

	return nil
}

// ValidateSecurityConfig validates security configuration against protocol
func ValidateSecurityConfig(protocol string, security SecurityConfig) error {
	validModes := validSecurityModes[protocol]
	if validModes == nil {
		return fmt.Errorf("unknown protocol: %s", protocol)
	}

	// Check if security mode is valid for protocol
	modeValid := slices.Contains(validModes, security.Mode)

	if !modeValid {
		return fmt.Errorf("security mode %s not valid for protocol %s, must be one of: %s",
			security.Mode, protocol, strings.Join(validModes, ", "))
	}

	// Validate mode-specific requirements
	switch security.Mode {
	case "psk":
		if security.PSKIdentity == "" {
			return fmt.Errorf("psk_identity is required for PSK mode")
		}
		if security.PSKKey == "" {
			return fmt.Errorf("psk_key is required for PSK mode")
		}
		if len(security.PSKKey) < 4 {
			return fmt.Errorf("psk_key too short (minimum 4 characters)")
		}

	case "certificate":
		if security.CertFile == "" {
			return fmt.Errorf("cert_file is required for certificate mode")
		}
		if security.KeyFile == "" {
			return fmt.Errorf("key_file is required for certificate mode")
		}

		// Validate certificate files exist and are readable
		if err := validateFileExists(security.CertFile); err != nil {
			return fmt.Errorf("cert_file: %w", err)
		}
		if err := validateFileExists(security.KeyFile); err != nil {
			return fmt.Errorf("key_file: %w", err)
		}

		// Validate CA cert file if specified
		if security.CACertFile != "" {
			if err := validateFileExists(security.CACertFile); err != nil {
				return fmt.Errorf("ca_cert_file: %w", err)
			}
		}
	}

	return nil
}

// ValidateResourcePath validates a CoAP resource path
func ValidateResourcePath(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must start with /")
	}

	if len(path) > 255 {
		return fmt.Errorf("path too long (maximum 255 characters)")
	}

	// Allow wildcards (+) in paths for observations
	if !pathPattern.MatchString(path) {
		return fmt.Errorf("invalid path format")
	}

	// Validate path segments
	segments := strings.SplitSeq(path[1:], "/") // Remove leading /
	for segment := range segments {
		if segment == "" {
			continue // Allow empty segments (double slashes)
		}

		if len(segment) > 63 {
			return fmt.Errorf("path segment too long: %s (maximum 63 characters)", segment)
		}
	}

	return nil
}

// Helper functions

func getExpectedScheme(protocol string) string {
	switch protocol {
	case "udp":
		return "coap"
	case "tcp":
		return "coap"
	case "udp-dtls":
		return "coaps"
	case "tcp-tls":
		return "coaps"
	default:
		return "coap"
	}
}

func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func validateFileExists(filename string) error {
	if filename == "" {
		return fmt.Errorf("filename cannot be empty")
	}

	// Expand environment variables and home directory
	expanded := os.ExpandEnv(filename)
	if strings.HasPrefix(expanded, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("cannot determine home directory: %w", err)
		}
		expanded = filepath.Join(home, expanded[2:])
	}

	// Check if file exists and is readable
	info, err := os.Stat(expanded)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", expanded)
		}
		return fmt.Errorf("cannot access file %s: %w", expanded, err)
	}

	if info.IsDir() {
		return fmt.Errorf("path is a directory, not a file: %s", expanded)
	}

	// Check if file is readable
	file, err := os.Open(expanded)
	if err != nil {
		return fmt.Errorf("file is not readable: %s (%w)", expanded, err)
	}
	defer file.Close()

	// Attempt to read at least one byte
	buffer := make([]byte, 1)
	_, err = file.Read(buffer)
	if err != nil && err.Error() != "EOF" { // EOF is acceptable for empty files
		return fmt.Errorf("error reading file %s: %w", expanded, err)
	}

	return nil
}
