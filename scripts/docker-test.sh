# scripts/docker-test.sh
#!/bin/bash
set -e

echo "Testing Docker image..."

# Build test image
docker build -t benthos-coap:test .

# Create test network
docker network create coap-test-net 2>/dev/null || true

# Start mock CoAP server
echo "Starting mock CoAP server..."
docker run -d --name coap-server --network coap-test-net -p 5683:5683/udp eclipse/californium:latest

# Wait for server to start
sleep 5

# Test configuration
cat > /tmp/test-config.yaml << 'EOF'
input:
  generate:
    mapping: 'root = {"test": "data", "timestamp": now()}'
    interval: "1s"
    count: 5

output:
  coap:
    endpoints: ["coap://coap-server:5683"]
    default_path: "/test"
    protocol: "udp"
EOF

# Run test
echo "Running test..."
docker run --rm --network coap-test-net -v /tmp/test-config.yaml:/config.yaml benthos-coap:test -c /config.yaml

# Cleanup
echo "Cleaning up..."
docker stop coap-server || true
docker rm coap-server || true
docker network rm coap-test-net || true
docker rmi benthos-coap:test

echo "✅ Docker test completed successfully!"

---
