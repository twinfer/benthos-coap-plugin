# Benthos CoAP Plugin

[![CI](https://github.com/your-org/benthos-coap/workflows/CI/badge.svg)](https://github.com/your-org/benthos-coap/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/your-org/benthos-coap)](https://goreportcard.com/report/github.com/your-org/benthos-coap)
[![GoDoc](https://godoc.org/github.com/your-org/benthos-coap?status.svg)](https://godoc.org/github.com/your-org/benthos-coap)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Coverage](https://codecov.io/gh/your-org/benthos-coap/branch/main/graph/badge.svg)](https://codecov.io/gh/your-org/benthos-coap)

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
curl -sSL https://raw.githubusercontent.com/your-org/benthos-coap/main/scripts/install.sh | bash

# Or specify version
VERSION=v1.0.0 curl -sSL https://raw.githubusercontent.com/your-org/benthos-coap/main/scripts/install.sh | bash
```

#### Docker
```bash
docker pull ghcr.io/your-org/benthos-coap:latest
```

#### Build from Source
```bash
git clone https://github.com/your-org/benthos-coap.git
cd benthos-coap
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
      max_size: 10
      idle_timeout: "30s"
      health_check_interval: "10s"
    
    # Observer configuration
    observer:
      buffer_size: 1000
      observe_timeout: "5m"
    
    # Retry and circuit breaker
    retry_policy:
      max_retries: 3
      initial_interval: "1s"
      max_interval: "30s"
      multiplier: 2.0
      jitter: true
    
    circuit_breaker:
      enabled: true
      failure_threshold: 5
      timeout: "30s"
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
      confirmable: true
      timeout: "30s"
      content_format: "application/json"
      auto_detect_format: true
    
    # Retry configuration
    retry_policy:
      max_retries: 3
      initial_interval: "500ms"
      max_interval: "10s"
      multiplier: 1.5
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
    connection_pool:
      max_size: 5
      health_check_interval: "10s"

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
    observer:
      buffer_size: 50000
    converter:
      compression_enabled: true
      max_payload_size: 2097152  # 2MB

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
    image: ghcr.io/your-org/benthos-coap:latest
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
        image: ghcr.io/your-org/benthos-coap:latest
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

The plugin exposes comprehensive metrics for monitoring:

```
# Connection metrics
coap_connections_active - Number of active connections
coap_connections_created_total - Total connections created
coap_connections_failed_total - Total connection failures

# Input metrics  
coap_input_messages_read_total - Total messages read
coap_input_messages_dropped_total - Total messages dropped
coap_input_observes_active - Number of active observations
coap_input_errors_total - Total input errors

# Output metrics
coap_output_messages_sent_total - Total messages sent
coap_output_messages_failed_total - Total send failures
coap_output_requests_total - Total CoAP requests
coap_output_requests_success_total - Successful requests
coap_output_requests_timeout_total - Request timeouts

# Circuit breaker metrics
coap_circuit_breaker_open - Number of open circuit breakers
```

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
    # ... your config
    converter:
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
    
    # Increase connection pool
    connection_pool:
      max_size: 20
      idle_timeout: "60s"
    
    # Large buffer for burst handling
    observer:
      buffer_size: 100000
    
    # Aggressive health checking
    connection_pool:
      health_check_interval: "5s"

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
    
    # Small buffer, immediate processing
    observer:
      buffer_size: 10
    
    # Fast health checks
    connection_pool:
      health_check_interval: "1s"

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

```bash
# Unit tests
go test ./...

# Integration tests with mock server
go test -tags=integration ./pkg/testing/...

# Benchmarks
go test -bench=. ./...

# Test coverage
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

- 📖 [Documentation](https://your-org.github.io/benthos-coap)
- 🐛 [Issue Tracker](https://github.com/your-org/benthos-coap/issues)
- 💬 [Discussions](https://github.com/your-org/benthos-coap/discussions)
- 📧 [Email Support](mailto:support@your-org.com)

## Acknowledgments

- [Benthos](https://benthos.dev) - The stream processing framework
- [go-coap](https://github.com/plgd-dev/go-coap) - The CoAP library
- [CoAP RFC 7252](https://tools.ietf.org/html/rfc7252) - The CoAP specification