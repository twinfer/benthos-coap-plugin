# scripts/health-check.sh
#!/bin/bash
set -e

# Health check script for monitoring
BENTHOS_URL=${BENTHOS_URL:-http://localhost:4195}
TIMEOUT=${TIMEOUT:-10}

check_endpoint() {
    local endpoint=$1
    local description=$2
    
    if curl -s --max-time $TIMEOUT "$BENTHOS_URL$endpoint" > /dev/null; then
        echo "✅ $description"
        return 0
    else
        echo "❌ $description"
        return 1
    fi
}

echo "Performing health checks for Benthos CoAP Plugin..."

FAILED=0

# Basic ping
check_endpoint "/ping" "Basic health check" || FAILED=1

# Ready check (input/output connected)
check_endpoint "/ready" "Ready check (I/O connected)" || FAILED=1

# Metrics endpoint
check_endpoint "/stats" "Metrics endpoint" || FAILED=1

# Check specific CoAP metrics
STATS=$(curl -s --max-time $TIMEOUT "$BENTHOS_URL/stats" 2>/dev/null || echo "{}")

# Extract key metrics
CONNECTIONS=$(echo "$STATS" | jq -r '.coap_connections_active // "unknown"' 2>/dev/null || echo "unknown")
MESSAGES_READ=$(echo "$STATS" | jq -r '.coap_input_messages_read_total // "unknown"' 2>/dev/null || echo "unknown")
ERRORS=$(echo "$STATS" | jq -r '.coap_input_errors_total // "unknown"' 2>/dev/null || echo "unknown")

echo ""
echo "CoAP Plugin Metrics:"
echo "  Active Connections: $CONNECTIONS"
echo "  Messages Read: $MESSAGES_READ"
echo "  Errors: $ERRORS"

if [ $FAILED -eq 0 ]; then
    echo ""
    echo "✅ All health checks passed"
    exit 0
else
    echo ""
    echo "❌ Some health checks failed"
    exit 1
fi

---

