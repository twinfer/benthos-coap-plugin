# scripts/config-validator.sh
#!/bin/bash
set -e

CONFIG_FILE=${1:-config.yaml}

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Configuration file not found: $CONFIG_FILE"
    exit 1
fi

echo "Validating Benthos CoAP configuration: $CONFIG_FILE"

# Use benthos lint command to validate
if ./bin/benthos-coap lint "$CONFIG_FILE"; then
    echo "✅ Configuration is valid"
    
    # Additional CoAP-specific validations
    echo "Performing CoAP-specific validations..."
    
    # Check for required fields
    if grep -q "coap:" "$CONFIG_FILE"; then
        echo "✅ CoAP plugin found in configuration"
        
        # Check for endpoints
        if grep -q "endpoints:" "$CONFIG_FILE"; then
            echo "✅ Endpoints configured"
        else
            echo "⚠️  Warning: No endpoints found"
        fi
        
        # Check protocol
        PROTOCOL=$(grep "protocol:" "$CONFIG_FILE" | head -1 | sed 's/.*protocol: *"\?\([^"]*\)"\?.*/\1/')
        if [[ "$PROTOCOL" =~ ^(udp|tcp|udp-dtls|tcp-tls)$ ]]; then
            echo "✅ Valid protocol: $PROTOCOL"
        else
            echo "❌ Invalid protocol: $PROTOCOL"
            exit 1
        fi
        
        # Check security configuration for secure protocols
        if [[ "$PROTOCOL" == "udp-dtls" || "$PROTOCOL" == "tcp-tls" ]]; then
            if grep -q "security:" "$CONFIG_FILE"; then
                echo "✅ Security configuration found for secure protocol"
            else
                echo "❌ Security configuration required for protocol: $PROTOCOL"
                exit 1
            fi
        fi
        
    else
        echo "ℹ️  No CoAP plugin configuration found"
    fi
    
    echo "✅ All validations passed"
else
    echo "❌ Configuration validation failed"
    exit 1
fi