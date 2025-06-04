# Benthos CoAP Plugin

[![CI](https://github.com/twinfer/benthos-coap-plugin/workflows/CI/badge.svg)](https://github.com/twinfer/benthos-coap-plugin/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/twinfer/benthos-coap-plugin)](https://goreportcard.com/report/github.com/twinfer/benthos-coap-plugin)
[![GoDoc](https://godoc.org/github.com/twinfer/benthos-coap-plugin?status.svg)](https://godoc.org/github.com/twinfer/benthos-coap-plugin)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Coverage](https://codecov.io/gh/twinfer/benthos-coap-plugin/branch/main/graph/badge.svg)](https://codecov.io/gh/twinfer/benthos-coap-plugin)

A high-performance CoAP (Constrained Application Protocol) plugin for [Benthos](https://benthos.dev) that enables seamless integration with IoT devices and constrained networks. This plugin provides both input and output components with comprehensive support for UDP, TCP, DTLS, and TCP-TLS protocols.

## Features

### 🔌 **Protocol Support**
- **UDP CoAP** - Standard connectionless CoAP (RFC 7252)
- **TCP CoAP** - Reliable transport variant (RFC 8323)
- **DTLS CoAP** - Secure UDP with DTLS encryption
- **TCP-TLS CoAP** - Secure TCP with TLS encryption

### 📡 **Input Capabilities**
- **CoAP Observe** - Real-time subscriptions to resource changes
- **Automatic Reconnection** - Robust connection management with exponential backoff
- **Circuit Breaker** - Prevents cascading failures with configurable thresholds
- **Connection Pooling** - Efficient resource utilization and load balancing
- **Health Monitoring** - Continuous endpoint health checks

### 📤 **Output Capabilities**
- **Request/Response** - Standard CoAP request patterns (GET, POST, PUT, DELETE)
- **Batch Processing** - Efficient handling of message batches
- **Retry Logic** - Configurable retry policies with jitter
- **Load Balancing** - Distribute requests across multiple endpoints

### 🔒 **Security Features**
- **PSK Authentication** - Pre-shared key support for DTLS
- **Certificate Authentication** - X.509 certificate-based authentication
- **Flexible Configuration** - Support for custom cipher suites and verification modes

### 📊 **Observability**
- **Prometheus Metrics** - Comprehensive metrics for monitoring
- **Health Checks** - Built-in health endpoints
- **Structured Logging** - Detailed logging with correlation IDs
- **Distributed Tracing** - OpenTelemetry compatible tracing

## Quick Start

### Installation

#### Download Binary
```bash
# Install latest version
curl -sSL https://raw.githubusercontent.com/twinfer/benthos-coap-plugin/main/scripts/install.sh | bash

# Or specify version
VERSION=v1.0.0 curl -sSL https://raw.githubusercontent.com/twinfer/benthos-coap-plugin/main/scripts/install.sh | bash
```

#### Docker
```bash
docker pull ghcr.io/twinfer/benthos-coap-plugin:latest
```

#### Build from Source
```bash
git clone https://github.com/twinfer/benthos-coap-plugin.git
cd benthos-coap-plugin
make build
```

### Basic Usage

#### IoT Sensor Data Ingestion
```yaml
input:
  coap:
    endpoints:
      - "coap://iot-sensor:5683"
    observe_paths:
      - "/sensors/temperature"
      - "/sensors/humidity"

pipeline:
  processors:
    - mapping: |
        root.device_id = this.metadata.coap_uri_path.split("/")[2]
        root.timestamp = now()
        root.data = this.content().parse_json()

output:
  kafka:
    addresses: ["kafka:9092"]
    topic: "sensor-data"
```

#### Device Command Distribution
```yaml
input:
  kafka:
    addresses: ["kafka:9092"]
    topics: ["device-commands"]

pipeline:
  processors:
    - mapping: |
        meta coap_path = "/actuators/" + this.device_id + "/command"
        root = this.command_data

output:
  coap:
    endpoints:
      - "coap://device-gateway:5683"
    protocol: "udp"
    request_options:
      confirmable: true
```

## Configuration Reference

### Input Configuration

```yaml
input:
  coap:
    # Required: List of CoAP endpoints
    endpoints:
      - "coap://device1:5683"
      - "coaps://device2:5684"
    
    # Required: Resource paths to observe
    observe_paths:
      - "/sensors/+/data"  # Wildcard support
      - "/status"
    
    # Protocol selection
    protocol: "udp"  # udp, tcp, udp-dtls, tcp-tls
    
    # Security configuration
    security:
      mode: "psk"  # none, psk, certificate
      psk_identity: "client1"
      psk_key: "${COAP_PSK_KEY}"
      # For certificate mode:
      # cert_file: "/etc/ssl/client.crt"
      # key_file: "/etc/ssl/client.key"
    
    # Connection pool settings
    connection_pool:
      max_size: 5 # Max connections per endpoint
      idle_timeout: "30s" # How long to keep idle connections open
      health_check_interval: "10s" # Interval for connection health checks
      connect_timeout: "10s" # Timeout for establishing new connections
    
    # Observer configuration
    observer:
      buffer_size: 1000 # Message buffer for observe notifications
      observe_timeout: "5m" # Timeout for individual observe operations
      resubscribe_delay: "5s" # Delay before resubscribing after failure
    
    # Retry policy for underlying operations like initial connection or observe setup attempts
    retry_policy:
      max_retries: 3
      initial_interval: "1s"
      max_interval: "30s"
      multiplier: 2.0
      jitter: true # Adds randomness to retry intervals
    
    circuit_breaker:
      enabled: true
      failure_threshold: 5 # Failures to open circuit
      success_threshold: 3 # Successes to close circuit (from half-open)
      timeout: "30s" # Time before moving from open to half-open
      half_open_max_calls: 2 # Max calls in half-open state to test recovery

    # Message conversion settings
    converter:
      default_content_format: "application/json" # For messages without explicit format
      # compression_enabled: true # Enable payload compression
      # max_payload_size: 1048576 # Max payload size in bytes (1MB)
      preserve_options: false # Preserve all CoAP options in message metadata
```

### Output Configuration

```yaml
output:
  coap:
    # Required: List of CoAP endpoints
    endpoints:
      - "coap://gateway:5683"
    
    # Default path for messages without explicit path
    default_path: "/data/events"
    
    # Protocol selection
    protocol: "udp"
    
    # Request options
    request_options:
      confirmable: true # Send confirmable messages (expect ACK)
      default_method: "POST" # GET, POST, PUT, DELETE
      timeout: "30s" # Request timeout for confirmable messages
      content_format: "application/json" # Default if not set by message or auto_detect_format
      auto_detect_format: true # Auto-detect from "content-type" metadata or payload characteristics
    
    # Retry policy for failed requests
    retry_policy:
      max_retries: 3
      initial_interval: "500ms"
      max_interval: "10s"
      multiplier: 1.5
      jitter: true # Adds randomness to retry intervals

    # Security configuration (same structure as input.coap)
    # security:
      # mode: "none"  # none, psk, certificate
      # psk_identity: "client_output"
      # psk_key: "${COAP_OUTPUT_PSK_KEY}"
      # cert_file: "/etc/ssl/client_output.crt"
      # key_file: "/etc/ssl/client_output.key"
      # ca_cert_file: "/etc/ssl/ca.crt"
      # insecure_skip_verify: false

    # Connection pool settings (same structure as input.coap)
    # connection_pool:
      # max_size: 5
      # idle_timeout: "30s"
      # health_check_interval: "10s"
      # connect_timeout: "10s"

    # Message conversion settings (same structure as input.coap)
    # converter:
      # default_content_format: "application/json"
      # compression_enabled: true # Enable payload compression
      # max_payload_size: 1048576 # Max payload size in bytes (1MB)
      # preserve_options: false
```

## Advanced Usage

### Secure DTLS Communication

```yaml
input:
  coap:
    endpoints:
      - "coaps://secure-device:5684"
    observe_paths:
      - "/secure/data"
    protocol: "udp-dtls"
    security:
      mode: "psk"
      psk_identity: "sensor-001"
      psk_key: "${DTLS_PSK_SECRET}"
      # ca_cert_file and insecure_skip_verify are not used for PSK mode
    connection_pool:
      max_size: 5 # Default
      idle_timeout: "30s" # Default
      health_check_interval: "10s"
      connect_timeout: "10s" # Default
    # Other sections like observer, retry_policy, circuit_breaker, converter use defaults

pipeline:
  processors:
    - mapping: |
        root = this.content().parse_json()
        root.security_level = "high"
        root.received_at = now()

output:
  kafka:
    addresses: ["kafka:9092"]
    topic: "secure-sensor-data"
```

### Certificate-Based Authentication

```yaml
input:
  coap:
    endpoints:
      - "coaps://enterprise-iot:5684"
    observe_paths:
      - "/enterprise/+/metrics"
    protocol: "tcp-tls"
    security:
      mode: "certificate"
      cert_file: "/etc/ssl/client.crt"
      key_file: "/etc/ssl/client.key"
      ca_cert_file: "/etc/ssl/ca.crt"
      insecure_skip_verify: false
```

### High-Throughput Batch Processing

```yaml
input:
  coap:
    endpoints:
      - "coap://high-volume-sensor:5683"
    observe_paths:
      - "/stream/data"
    protocol: "tcp"  # TCP for higher throughput
    connection_pool: # Merged from later in the original example
      max_size: 5 # Default, can be overridden
      idle_timeout: "30s" # Default
      health_check_interval: "5s" # User override from original example
      connect_timeout: "10s" # Default
    observer:
      buffer_size: 50000 # User override
      # observe_timeout: "5m" # Default
      # resubscribe_delay: "5s" # Default
    converter:
      # default_content_format: "application/json" # Default
      compression_enabled: true # User override
      max_payload_size: 2097152  # 2MB, user override
      # preserve_options: false # Default
    # Other sections like security, retry_policy, circuit_breaker use defaults

pipeline:
  processors:
    - batch:
        count: 1000
        period: "5s"
    - archive:
        format: "json_array"
    - compress:
        algorithm: "gzip"

output:
  aws_s3:
    bucket: "iot-data-lake"
    path: "coap-data/${! timestamp_unix() }.json.gz"
```

### Multi-Protocol Integration

```yaml
input:
  broker:
    inputs:
      # UDP sensors
      - coap:
          endpoints: ["coap://udp-sensors:5683"]
          observe_paths: ["/udp/+/data"]
          protocol: "udp"
      
      # TCP devices  
      - coap:
          endpoints: ["coap://tcp-devices:5683"]
          observe_paths: ["/tcp/+/events"]
          protocol: "tcp"
      
      # Secure DTLS gateways
      - coap:
          endpoints: ["coaps://secure-gw:5684"]
          observe_paths: ["/secure/+/alerts"]
          protocol: "udp-dtls"
          security:
            mode: "psk"
            psk_identity: "monitor"
            psk_key: "${DTLS_PSK}"

pipeline:
  processors:
    - switch:
        - check: 'this.metadata.coap_uri_path.contains("/alerts")'
          processors:
            - mapping: 'root.priority = "high"'
        - processors:
            - mapping: 'root.priority = "normal"'

output:
  switch:
    cases:
      - check: 'this.priority == "high"'
        output:
          http_client:
            url: "http://alert-manager:9093/api/v1/alerts"
      - output:
          kafka:
            addresses: ["kafka:9092"]
            topic: "coap-events"
```

## Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  benthos-coap:
    image: ghcr.io/twinfer/benthos-coap-plugin:latest
    volumes:
      - ./config.yaml:/config.yaml
      - ./ssl:/etc/ssl:ro
    ports:
      - "4195:4195"  # Benthos HTTP server
    environment:
      - BENTHOS_LOG_LEVEL=INFO
      - COAP_PSK_KEY=supersecretkey
    command: ["-c", "/config.yaml"]
    restart: unless-stopped
    
    # Health check
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4195/ping"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Your CoAP devices/gateways
  coap-server:
    image: eclipse/californium:latest
    ports:
      - "5683:5683/udp"
      - "5684:5684/udp"
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: benthos-coap
spec:
  replicas: 3
  selector:
    matchLabels:
      app: benthos-coap
  template:
    metadata:
      labels:
        app: benthos-coap
    spec:
      containers:
      - name: benthos-coap
        image: ghcr.io/twinfer/benthos-coap-plugin:latest
        ports:
        - containerPort: 4195
          name: http
        env:
        - name: BENTHOS_LOG_LEVEL
          value: "INFO"
        - name: COAP_PSK_KEY
          valueFrom:
            secretKeyRef:
              name: coap-secrets
              key: psk-key
        volumeMounts:
        - name: config
          mountPath: /config.yaml
          subPath: config.yaml
        - name: ssl-certs
          mountPath: /etc/ssl
          readOnly: true
        livenessProbe:
          httpGet:
            path: /ping
            port: 4195
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 4195
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
      volumes:
      - name: config
        configMap:
          name: benthos-coap-config
      - name: ssl-certs
        secret:
          secretName: coap-ssl-certs

---
apiVersion: v1
kind: Service
metadata:
  name: benthos-coap
spec:
  selector:
    app: benthos-coap
  ports:
  - port: 4195
    targetPort: 4195
    name: http

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: benthos-coap-config
data:
  config.yaml: |
    input:
      coap:
        endpoints: ["coap://coap-devices:5683"]
        observe_paths: ["/sensors/+/data"]
    output:
      kafka:
        addresses: ["kafka:9092"]
        topic: "iot-data"
```

## Monitoring

### Prometheus Metrics

The plugin exposes comprehensive metrics for monitoring (actual names might be prefixed by Benthos, e.g., `benthos_input_coap_messages_read_total`):

```
# Input Plugin Metrics (input.coap)
coap_input_messages_read_total - Total messages successfully read from CoAP observations and forwarded.
coap_input_messages_dropped_total - Total messages dropped from the input buffer (e.g., due to a full buffer).
coap_input_connections_open - Number of currently open connections by the input plugin.
coap_input_observes_active - Number of active CoAP observe subscriptions.
coap_input_errors_total - Total errors encountered by the input plugin (e.g., connection errors, subscription failures).

# Output Plugin Metrics (output.coap)
coap_output_messages_sent_total - Total messages successfully sent as CoAP requests.
coap_output_messages_failed_total - Total messages that failed to be sent after all retries.
coap_output_requests_total - Total CoAP requests initiated by the output plugin.
coap_output_requests_success_total - Total CoAP requests that completed successfully (e.g., received a 2.xx response).
coap_output_requests_timeout_total - Total CoAP requests that timed out.
coap_output_retries_total - Total number of retry attempts made for failed CoAP requests.
coap_output_connections_used - Counter for how many times connections were obtained from the pool for sending requests.
```
Note: The `_total` suffix is typically added by Prometheus for counter metrics. Benthos registers the base name (e.g., `coap_input_messages_read`).

### Grafana Dashboard

Import the provided Grafana dashboard for comprehensive monitoring:

```json
{
  "dashboard": {
    "title": "Benthos CoAP Plugin",
    "panels": [
      {
        "title": "Message Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(coap_input_messages_read_total[5m])",
            "legendFormat": "Messages In/sec"
          },
          {
            "expr": "rate(coap_output_messages_sent_total[5m])",
            "legendFormat": "Messages Out/sec"
          }
        ]
      },
      {
        "title": "Connection Health",
        "type": "stat",
        "targets": [
          {
            "expr": "coap_connections_active",
            "legendFormat": "Active Connections"
          }
        ]
      }
    ]
  }
}
```

## Troubleshooting

### Common Issues

#### Connection Timeouts
```
Error: failed to connect to CoAP endpoint
```
**Solution:**
- Verify endpoint is reachable: `nc -zu <host> <port>`
- Check firewall rules
- Increase connection timeout in configuration
- Verify protocol (UDP vs TCP)

#### DTLS Authentication Failures
```
Error: DTLS handshake failed
```
**Solution:**
- Verify PSK identity and key match server configuration
- Check certificate validity and paths
- Ensure compatible cipher suites
- Enable debug logging: `BENTHOS_LOG_LEVEL=DEBUG`

#### High Memory Usage
```
Warning: message buffer full, dropping messages
```
**Solution:**
- Increase `observer.buffer_size`
- Add more processing threads
- Implement backpressure handling
- Scale horizontally

#### Observer Disconnections
```
Warning: CoAP observe subscription lost
```
**Solution:**
- Check network stability
- Verify server supports observe
- Adjust `observe_timeout`
- Enable circuit breaker

### Debug Mode

Enable detailed logging for troubleshooting:

```yaml
logger:
  level: DEBUG
  format: json

input:
  coap:
    # ... your config (endpoints, observe_paths are required for a functional input)
    # Example:
    # endpoints: ["coap://localhost:5683"]
    # observe_paths: ["/debug/path"]
    converter:
      # default_content_format: "application/json" # Default
      # compression_enabled: true # Default
      # max_payload_size: 1048576 # Default
      preserve_options: true  # Keep all CoAP options for debugging

pipeline:
  processors:
    - log:
        level: DEBUG
        message: "CoAP message: ${! content() }"
```

### Health Checks

Monitor plugin health:

```bash
# Check if plugin is healthy
curl http://localhost:4195/ping

# Check if input/output are connected
curl http://localhost:4195/ready

# View metrics
curl http://localhost:4195/stats
```

## Performance Tuning

### High Throughput Configuration

```yaml
input:
  coap:
    # Use TCP for better throughput
    protocol: "tcp"
    
    # Example placeholder, replace with actual endpoints
    endpoints: ["coap://example-high-throughput:5683"]
    observe_paths: ["/stream/all"] # Example placeholder

    # Increase connection pool
    connection_pool:
      max_size: 20 # User override
      idle_timeout: "60s" # User override
      health_check_interval: "5s" # User override
      connect_timeout: "10s" # Default
    
    # Large buffer for burst handling
    observer:
      buffer_size: 100000 # User override
      observe_timeout: "5m" # Default
      resubscribe_delay: "5s" # Default
    # Other sections like security, retry_policy, circuit_breaker, converter use defaults

# Use multiple processing threads  
pipeline:
  threads: 4
  processors:
    - mapping: |
        # Lightweight processing
        root = this

# Batch output for efficiency
output:
  kafka:
    addresses: ["kafka:9092"]
    topic: "iot-data"
    batching:
      count: 1000
      period: "1s"
```

### Low Latency Configuration

```yaml
input:
  coap:
    # UDP for lowest latency
    protocol: "udp"
    
    # Example placeholder, replace with actual endpoints
    endpoints: ["coap://example-low-latency:5683"]
    observe_paths: ["/realtime/updates"] # Example placeholder

    # Small buffer, immediate processing
    observer:
      buffer_size: 10 # User override
      observe_timeout: "5m" # Default
      resubscribe_delay: "5s" # Default
    
    # Fast health checks, other pool settings default
    connection_pool:
      max_size: 5 # Default
      idle_timeout: "30s" # Default
      health_check_interval: "1s" # User override
      connect_timeout: "10s" # Default
    # Other sections like security, retry_policy, circuit_breaker, converter use defaults

# Single thread, no batching
pipeline:
  threads: 1
  processors: []

output:
  http_client:
    url: "http://realtime-api:8080/events"
    verb: "POST"
    headers:
      Content-Type: "application/json"
```

## Development

### Building

```bash
# Install dependencies
make deps

# Run tests
make test

# Run integration tests
make test-integration

# Build binary
make build

# Build for all platforms
make build-all

# Run linter
make lint
```

### Testing

The testing strategy for this plugin includes unit tests for core logic and comprehensive integration tests to ensure reliable operation with CoAP services.

#### Unit Tests
- Core logic for connection management, observation handling, and message conversion is unit-tested.
- Error handling paths are validated.
- Configuration parsing and validation are checked.
- General command: `go test ./...`

#### **Mock CoAP Server (`pkg/testing/mock_server.go`)**
An in-memory CoAP server designed for testing Benthos plugins and other applications that interact with CoAP services. It allows for simulating various server behaviors without needing a real CoAP device or server.

**Key Features:**
- Define and manage mock CoAP resources, including their paths, content types, and initial data.
- Handle standard CoAP methods: GET, POST, PUT, DELETE.
- Support for CoAP Observe: Mark resources as observable and simulate notifications on updates.
- Request History: Capture a detailed log of incoming requests (path, method, token, options, payload) for assertions in tests.
- Response Customization: Simulate server-side delays (`ResponseDelay`) and sequences of different response codes (`ResponseSequence`) for specific resources to test client resilience and retry logic.

The mock server can be started programmatically in Go tests. Resources can be added, and its request history can be inspected to verify interactions. The `MockCoAPServer` is the backbone of the integration tests for both the CoAP input and output plugins, allowing for a wide range of scenarios to be simulated and verified.

#### **Plugin Integration Tests**
The CoAP input (`pkg/input/coap_input_plugin.go`) and output (`pkg/output/coap_output_plugin.go`) plugins within this repository are accompanied by a comprehensive suite of integration tests.
These tests are located in `pkg/input/coap_input_integration_test.go` and `pkg/output/coap_output_integration_test.go`.
They utilize the `MockCoAPServer` to validate plugin behavior under various conditions, including correct message processing, CoAP option handling, observation flows, error handling, and retry mechanisms.

- **Command to run integration tests**: `go test -tags=integration ./pkg/...` (or specify individual plugin test files).

```bash
# Run unit tests
go test ./...

# Run integration tests (includes tests for pkg/testing, pkg/input, pkg/output)
go test -tags=integration ./pkg/...

# Run benchmarks
go test -bench=. ./...

# Generate and view test coverage
make coverage
```

### Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://twinfer.github.io/benthos-coap-plugin)
- 🐛 [Issue Tracker](https://github.com/twinfer/benthos-coap-plugin/issues)
- 💬 [Discussions](https://github.com/twinfer/benthos-coap-plugin/discussions)
- 📧 [Email Support](mailto:support@example.com)

## Acknowledgments

- [Benthos](https://benthos.dev) - The stream processing framework
- [go-coap](https://github.com/plgd-dev/go-coap) - The CoAP library
- [CoAP RFC 7252](https://tools.ietf.org/html/rfc7252) - The CoAP specification