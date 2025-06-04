# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Building
```bash
make build              # Build single platform binary
make build-all          # Build for multiple platforms (Linux, macOS, Windows)
go build -o bin/benthos-coap ./cmd
```

### Testing
```bash
make test               # Run unit tests with race detection and coverage
make test-integration   # Run integration tests with mock CoAP servers
make bench              # Run performance benchmarks
make coverage           # Generate HTML coverage report
go test -timeout 10m -race -coverprofile=coverage.out ./...
```

### Code Quality
```bash
make lint               # Run golangci-lint
make fmt                # Format code with go fmt
make vet                # Run go vet
```

### Development Setup
```bash
make dev-setup          # Install development tools (golangci-lint, goreleaser)
make deps               # Download and tidy dependencies
```

### Running Examples
```bash
make run-example        # Run with example configuration
./bin/benthos-coap -c examples/input-sensors.yaml
```

## Architecture Overview

This is a Benthos v4 plugin that adds CoAP (Constrained Application Protocol) support for IoT communication. The plugin provides both input and output components for real-time IoT data processing.

### Core Package Structure

- **`pkg/input/`** - CoAP input plugin for observing sensor data streams via CoAP observe subscriptions
- **`pkg/output/`** - CoAP output plugin for sending commands/data to IoT devices  
- **`pkg/connection/`** - Connection pool management with health checks and load balancing
- **`pkg/converter/`** - Bidirectional message conversion between Benthos and CoAP formats
- **`pkg/observer/`** - CoAP observe subscription management with circuit breaker patterns
- **`pkg/config/`** - Configuration validation, defaults, and security settings
- **`pkg/metrics/`** - Prometheus-compatible metrics collection

### Plugin Registration

The main entry point (`cmd/main_plugin_registration.go`) registers both input and output plugins with Benthos and starts the CLI. The plugins are automatically discovered through Go init() functions in their respective packages.

### Connection Management Architecture

The connection system supports:
- **Multi-protocol**: UDP, TCP, DTLS, TCP-TLS
- **Connection pooling**: Round-robin selection with health monitoring
- **Security**: PSK authentication, X.509 certificates, TLS/DTLS
- **Resilience**: Circuit breakers, automatic reconnection, retry policies

### Message Flow

1. **Input Plugin**: Establishes CoAP observe subscriptions → receives real-time updates → converts to Benthos messages
2. **Output Plugin**: Receives Benthos messages → converts to CoAP requests → sends to IoT endpoints

### Testing Strategy

- **Unit tests**: `pkg/*/` directories contain `*_test.go` files
- **Integration tests**: `pkg/testing/integration_test.go` with mock CoAP servers
- **Mock server**: `pkg/testing/mock_server.go` for testing without real IoT devices
- **Benchmarks**: Performance testing in `pkg/testing/` with memory/CPU profiling

### Security Considerations

The plugin handles sensitive IoT communications with built-in security:
- Certificate-based authentication for production deployments
- PSK (Pre-Shared Key) support for constrained devices  
- DTLS/TLS encryption for secure transport
- Configuration validation to prevent security misconfigurations

### Monitoring Integration

- **Prometheus metrics**: Connection health, message throughput, error rates
- **Grafana dashboards**: Pre-configured dashboards in `monitoring/grafana/`
- **Health checks**: Built-in health endpoints for service monitoring
- **Alerting**: Prometheus alert rules for common failure scenarios

 use Knowledge Graph to save API signuture for future refrence