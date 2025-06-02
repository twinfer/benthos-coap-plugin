# scripts/generate-certs.sh
#!/bin/bash
set -e

CERT_DIR="./certs"
mkdir -p $CERT_DIR

echo "Generating certificates for CoAP DTLS/TLS testing..."

# Generate CA private key
openssl genrsa -out $CERT_DIR/ca-key.pem 4096

# Generate CA certificate
openssl req -new -x509 -days 365 -key $CERT_DIR/ca-key.pem -sha256 -out $CERT_DIR/ca.pem -subj "/C=US/ST=CA/L=San Francisco/O=Benthos CoAP/CN=Test CA"

# Generate server private key
openssl genrsa -out $CERT_DIR/server-key.pem 4096

# Generate server certificate request
openssl req -subj "/C=US/ST=CA/L=San Francisco/O=Benthos CoAP/CN=localhost" -sha256 -new -key $CERT_DIR/server-key.pem -out $CERT_DIR/server.csr

# Generate server certificate
openssl x509 -req -days 365 -sha256 -in $CERT_DIR/server.csr -CA $CERT_DIR/ca.pem -CAkey $CERT_DIR/ca-key.pem -out $CERT_DIR/server.pem -CAcreateserial

# Generate client private key
openssl genrsa -out $CERT_DIR/client-key.pem 4096

# Generate client certificate request
openssl req -subj "/C=US/ST=CA/L=San Francisco/O=Benthos CoAP/CN=client" -new -key $CERT_DIR/client-key.pem -out $CERT_DIR/client.csr

# Generate client certificate
openssl x509 -req -days 365 -sha256 -in $CERT_DIR/client.csr -CA $CERT_DIR/ca.pem -CAkey $CERT_DIR/ca-key.pem -out $CERT_DIR/client.pem -CAcreateserial

# Clean up CSR files
rm $CERT_DIR/server.csr $CERT_DIR/client.csr

echo "✅ Certificates generated in $CERT_DIR/"
echo "Files created:"
echo "  ca.pem          - CA certificate"
echo "  server.pem      - Server certificate"
echo "  server-key.pem  - Server private key"
echo "  client.pem      - Client certificate"
echo "  client-key.pem  - Client private key"

---