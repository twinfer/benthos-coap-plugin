# scripts/load-test.sh
#!/bin/bash
set -e

echo "Running load test for Benthos CoAP Plugin..."

# Configuration
DURATION=${1:-60s}
RATE=${2:-100}
ENDPOINTS=${3:-1}

echo "Test parameters:"
echo "  Duration: $DURATION"
echo "  Rate: $RATE messages/sec"
echo "  Endpoints: $ENDPOINTS"

# Create load test configuration
cat > /tmp/load-test.yaml << EOF
input:
  generate:
    mapping: |
      root = {
        "id": uuid_v4(),
        "timestamp": now(),
        "data": range(0, 100).map_each(this.string()),
        "metadata": {
          "source": "load_test",
          "rate": $RATE
        }
      }
    interval: "${DURATION%s}ms" # Convert to milliseconds
    count: 0 # Infinite

pipeline:
  processors:
    - rate_limit:
        resource: "test_limiter"

rate_limit_resources:
  - label: "test_limiter"
    local:
      count: $RATE
      interval: "1s"

output:
  coap:
    endpoints:
$(for i in $(seq 1 $ENDPOINTS); do
    echo "      - \"coap://localhost:568$((2+i))\""
done)
    default_path: "/load-test/data"
    protocol: "udp"
    request_options:
      confirmable: false # Non-confirmable for higher throughput
    connection_pool:
      max_size: 10
EOF

# Start mock servers
PIDS=()
for i in $(seq 1 $ENDPOINTS); do
    PORT=$((5682+i))
    echo "Starting mock server on port $PORT..."
    go run ./pkg/testing/mock_server.go -port=$PORT &
    PIDS+=($!)
done

# Wait for servers to start
sleep 2

# Cleanup function
cleanup() {
    echo "Stopping load test..."
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    rm -f /tmp/load-test.yaml
}
trap cleanup EXIT

# Run load test
echo "Starting load test..."
timeout $DURATION ./bin/benthos-coap -c /tmp/load-test.yaml

echo "✅ Load test completed!"

---
