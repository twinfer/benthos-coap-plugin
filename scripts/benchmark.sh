# scripts/benchmark.sh
#!/bin/bash
set -e

echo "Running Benthos CoAP Plugin benchmarks..."

# Build optimized binary
echo "Building optimized binary..."
go build -ldflags="-s -w" -o bin/benthos-coap-bench ./cmd

# Start mock CoAP server
echo "Starting mock CoAP server..."
go run ./pkg/testing/mock_server.go &
MOCK_PID=$!

# Wait for server to start
sleep 2

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    kill $MOCK_PID 2>/dev/null || true
    rm -f bin/benthos-coap-bench
}
trap cleanup EXIT

# Run benchmarks
echo "Running throughput benchmark..."

# Create benchmark config
cat > /tmp/bench-config.yaml << 'EOF'
input:
  coap:
    endpoints: ["coap://localhost:5683"]
    observe_paths: ["/bench/data"]
    observer:
      buffer_size: 100000

pipeline:
  processors: []

output:
  drop: {}
EOF

# Memory benchmark
echo "Running memory benchmark..."
go test -bench=BenchmarkCoAPThroughput -benchmem -memprofile=mem.prof ./pkg/testing/...

# CPU profiling
echo "Running CPU profiling..."
go test -bench=BenchmarkCoAPThroughput -cpuprofile=cpu.prof ./pkg/testing/...

echo "✅ Benchmarks complete!"
echo "Results:"
echo "- Memory profile: mem.prof"
echo "- CPU profile: cpu.prof"
echo ""
echo "To analyze profiles:"
echo "  go tool pprof mem.prof"
echo "  go tool pprof cpu.prof"

---
