# Benthos CoAP Plugin Architecture

## Overview

This architecture implements both Input and Output plugins for Benthos using the `github.com/plgd-dev/go-coap/v3` package, supporting UDP, TCP, DTLS, and TCP-TLS protocols with CoAP observe subscriptions for real-time data streaming.

## Project Structure

```
benthos-coap/
├── pkg/
│   ├── input/           # CoAP Input plugin implementation
│   ├── output/          # CoAP Output plugin implementation
│   ├── connection/      # Connection pool management
│   ├── observer/        # CoAP observe subscription management
│   ├── converter/       # Message format conversion
│   ├── config/          # Configuration validation and parsing
│   └── metrics/         # Custom metrics collection
├── cmd/
│   └── main.go         # Plugin registration and CLI
├── examples/
│   ├── input.yaml      # Example input configurations
│   └── output.yaml     # Example output configurations
└── README.md
```

## Core Components

### 1. Connection Manager (`pkg/connection/manager.go`)

```go
type Manager struct {
    pools     map[string]*ConnectionPool
    config    Config
    logger    *service.Logger
    metrics   *service.Metrics
    mu        sync.RWMutex
}

type ConnectionPool struct {
    connections chan *ConnectionWrapper
    factory     ConnectionFactory
    maxSize     int
    currentSize int32
    metrics     *poolMetrics
}

type ConnectionWrapper struct {
    conn     *udp.Conn // or tcp.Conn
    lastUsed time.Time
    inUse    int32
    healthy  int32
}
```

**Key Features:**
- Protocol-agnostic connection pooling (UDP/TCP/DTLS/TCP-TLS)
- Health checking with automatic recovery
- Load balancing across multiple endpoints
- Graceful connection lifecycle management

### 2. Observer Manager (`pkg/observer/manager.go`)

```go
type Manager struct {
    subscriptions map[string]*Subscription
    connManager   *connection.Manager
    msgChan       chan *service.Message
    config        Config
    logger        *service.Logger
    ctx           context.Context
    cancel        context.CancelFunc
    mu            sync.RWMutex
}

type Subscription struct {
    path         string
    conn         *connection.ConnectionWrapper
    observeToken message.Token
    retryPolicy  RetryPolicy
    circuit      *CircuitBreaker
    lastSeen     time.Time
}
```

**Key Features:**
- CoAP observe subscription management
- Automatic re-subscription on failures
- Circuit breaker pattern for failing resources
- Token-based subscription tracking

### 3. Message Converter (`pkg/converter/converter.go`)

```go
type Converter struct {
    config Config
    logger *service.Logger
}

func (c *Converter) CoAPToMessage(msg *message.Message) (*service.Message, error)
func (c *Converter) MessageToCoAP(msg *service.Message) (*message.Message, error)
```

**Key Features:**
- Bidirectional message conversion
- MediaType handling (JSON, CBOR, XML, etc.)
- Metadata preservation
- Payload compression support

## Plugin Implementation

### CoAP Input Plugin (`pkg/input/coap.go`)

```go
func init() {
    configSpec := service.NewConfigSpec().
        Field(service.NewStringListField("endpoints")).
        Field(service.NewStringListField("observe_paths")).
        Field(service.NewStringField("protocol").Default("udp")).
        Field(service.NewObjectField("security",
            service.NewStringField("mode").Default("none"),
            service.NewStringField("psk_identity"),
            service.NewStringField("psk_key"),
            service.NewStringField("cert_file"),
            service.NewStringField("key_file"),
        )).
        Field(service.NewObjectField("batching",
            service.NewIntField("count").Default(1),
            service.NewDurationField("period").Default("1s"),
        ))

    err := service.RegisterInput("coap", configSpec, func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
        return newCoAPInput(conf, mgr)
    })
    if err != nil {
        panic(err)
    }
}

type Input struct {
    connManager *connection.Manager
    obsManager  *observer.Manager
    converter   *converter.Converter
    config      Config
    logger      *service.Logger
    metrics     *service.Metrics
    
    msgChan     chan *service.Message
    closeChan   chan struct{}
    closeOnce   sync.Once
}

func (i *Input) Connect(ctx context.Context) error {
    // Initialize connection manager
    // Start observer manager
    // Begin observation subscriptions
}

func (i *Input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
    select {
    case msg := <-i.msgChan:
        return msg, func(ctx context.Context, err error) error {
            // Handle acknowledgment
            return nil
        }, nil
    case <-ctx.Done():
        return nil, nil, ctx.Err()
    case <-i.closeChan:
        return nil, nil, service.ErrEndOfInput
    }
}

func (i *Input) Close(ctx context.Context) error {
    i.closeOnce.Do(func() {
        close(i.closeChan)
        i.obsManager.Close()
        i.connManager.Close()
    })
    return nil
}
```

### CoAP Output Plugin (`pkg/output/coap.go`)

```go
type Output struct {
    connManager *connection.Manager
    converter   *converter.Converter
    config      Config
    logger      *service.Logger
    metrics     *service.Metrics
}

func (o *Output) Connect(ctx context.Context) error {
    return o.connManager.Connect(ctx)
}

func (o *Output) Write(ctx context.Context, msg *service.Message) error {
    coapMsg, err := o.converter.MessageToCoAP(msg)
    if err != nil {
        return err
    }

    conn, err := o.connManager.Get(ctx)
    if err != nil {
        return err
    }
    defer o.connManager.Put(conn)

    // Send CoAP request with retry logic
    return o.sendWithRetry(ctx, conn, coapMsg)
}

func (o *Output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
    for _, msg := range batch {
        if err := o.Write(ctx, msg); err != nil {
            return err
        }
    }
    return nil
}
```

## Configuration Schema

### Input Configuration

```yaml
input:
  coap:
    endpoints:
      - "coap://iot-device-1:5683"
      - "coaps://iot-device-2:5684"
    observe_paths:
      - "/sensors/temperature"
      - "/sensors/humidity"
      - "/actuators/+/status"  # Wildcard support
    protocol: "udp"  # udp, tcp, udp-dtls, tcp-tls
    security:
      mode: "psk"  # none, psk, certificate
      psk_identity: "client1"
      psk_key: "secretkey123"
    connection_pool:
      max_size: 10
      idle_timeout: "30s"
      health_check_interval: "10s"
    batching:
      count: 100
      period: "1s"
    retry_policy:
      max_retries: 3
      initial_interval: "1s"
      max_interval: "30s"
      multiplier: 2.0
```

### Output Configuration

```yaml
output:
  coap:
    endpoints:
      - "coap://iot-gateway:5683"
    default_path: "/data/events"
    protocol: "udp"
    security:
      mode: "certificate"
      cert_file: "/etc/ssl/client.crt"
      key_file: "/etc/ssl/client.key"
    connection_pool:
      max_size: 5
      idle_timeout: "30s"
    retry_policy:
      max_retries: 5
      initial_interval: "500ms"
      max_interval: "10s"
      multiplier: 1.5
    request_options:
      confirmable: true
      timeout: "30s"
      content_format: "application/json"  # Auto-detected if not specified
```

## Error Handling Strategy

### 1. Connection Failures
- Exponential backoff with jitter
- Circuit breaker per endpoint
- Automatic failover to healthy endpoints
- Dead connection detection and cleanup

### 2. Message Processing Failures
- Immediate retry for transient errors
- Dead letter queue support via Benthos processors
- Detailed error metrics and logging
- Graceful degradation modes

### 3. Observer Failures
- Automatic re-subscription with backoff
- Subscription health monitoring
- Fallback to polling mode if observe fails
- Resource-level circuit breakers

## Security Implementation

### DTLS/TLS Support

```go
type SecurityConfig struct {
    Mode         string `yaml:"mode"`         // none, psk, certificate
    PSKIdentity  string `yaml:"psk_identity"`
    PSKKey       string `yaml:"psk_key"`
    CertFile     string `yaml:"cert_file"`
    KeyFile      string `yaml:"key_file"`
    CACertFile   string `yaml:"ca_cert_file"`
    InsecureSkip bool   `yaml:"insecure_skip_verify"`
}

func (c *Manager) createSecureConn(endpoint string, config SecurityConfig) (net.Conn, error) {
    switch config.Mode {
    case "psk":
        return c.createPSKConn(endpoint, config)
    case "certificate":
        return c.createTLSConn(endpoint, config)
    default:
        return c.createPlainConn(endpoint)
    }
}
```

## Scalability Features

### 1. Connection Pooling
- Configurable pool sizes per endpoint
- Connection reuse and lifecycle management
- Load balancing with round-robin/least-connections
- Health-based routing

### 2. Horizontal Scaling
- Stateless plugin design
- Distributed observer subscriptions
- Coordination-free operation
- Resource-based partitioning

### 3. Performance Optimizations
- Zero-copy message handling where possible
- Async I/O with configurable worker pools
- Message batching and aggregation
- Memory pool for frequent allocations

## Observability

### Metrics
```go
type Metrics struct {
    ConnectionsActive    prometheus.Gauge
    ConnectionsCreated   prometheus.Counter
    MessagesReceived     prometheus.Counter
    MessagesSent         prometheus.Counter
    ObservationsActive   prometheus.Gauge
    ErrorsTotal          prometheus.Counter
    LatencyHistogram     prometheus.Histogram
}
```

### Health Checks
- Connection pool health status
- Observer subscription status
- Endpoint availability
- Message processing rates

### Logging
- Structured logging with context
- Configurable log levels
- Error correlation IDs
- Performance tracing

## Usage Examples

### Stream IoT Sensor Data
```yaml
input:
  coap:
    endpoints: ["coap://sensor-network:5683"]
    observe_paths: ["/sensors/+/data"]
    batching:
      count: 50
      period: "5s"

pipeline:
  processors:
    - mapping: |
        root.device_id = this.metadata.coap_uri_path.split("/")[2]
        root.timestamp = now()
        root.data = this.content().parse_json()

output:
  kafka:
    addresses: ["kafka:9092"]
    topic: "iot-sensor-data"
```

### Send Commands to IoT Devices
```yaml
input:
  kafka:
    addresses: ["kafka:9092"]
    topics: ["device-commands"]

pipeline:
  processors:
    - mapping: |
        meta coap_path = "/actuators/" + this.device_id + "/command"
        root = this.command

output:
  coap:
    endpoints: ["coap://device-gateway:5683"]
    protocol: "udp"
```

## Testing Strategy

### Unit Tests
- Mock CoAP server for testing
- Connection manager test suite
- Message conversion validation
- Error handling verification

### Integration Tests
- Real CoAP server integration
- End-to-end message flow testing
- Security protocol validation
- Performance benchmarking

### Load Testing
- High-throughput observer scenarios
- Connection pool stress testing
- Memory usage profiling
- Latency measurements

This architecture provides a robust, scalable, and production-ready foundation for CoAP integration in Benthos pipelines, with proper separation of concerns, comprehensive error handling, and enterprise-grade security features.