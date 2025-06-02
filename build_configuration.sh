# Makefile
.PHONY: build test clean docker-build docker-push lint fmt vet deps

# Build variables
BINARY_NAME=benthos-coap
VERSION?=$(shell git describe --tags --always --dirty)
GIT_COMMIT=$(shell git rev-parse HEAD)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

# Docker variables
DOCKER_REGISTRY?=ghcr.io
DOCKER_REPOSITORY?=your-org/benthos-coap
DOCKER_TAG?=$(VERSION)
DOCKER_IMAGE=$(DOCKER_REGISTRY)/$(DOCKER_REPOSITORY):$(DOCKER_TAG)

# Go variables
GOOS?=linux
GOARCH?=amd64
CGO_ENABLED?=0

# Test variables
TEST_TIMEOUT=10m
TEST_COVERAGE_PROFILE=coverage.out

all: build

# Build the binary
build:
	@echo "Building $(BINARY_NAME) $(VERSION)"
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd

# Build for multiple platforms
build-all:
	@echo "Building for multiple platforms"
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 $(MAKE) build && mv bin/$(BINARY_NAME) bin/$(BINARY_NAME)-linux-amd64
	GOOS=linux GOARCH=arm64 $(MAKE) build && mv bin/$(BINARY_NAME) bin/$(BINARY_NAME)-linux-arm64
	GOOS=darwin GOARCH=amd64 $(MAKE) build && mv bin/$(BINARY_NAME) bin/$(BINARY_NAME)-darwin-amd64
	GOOS=darwin GOARCH=arm64 $(MAKE) build && mv bin/$(BINARY_NAME) bin/$(BINARY_NAME)-darwin-arm64
	GOOS=windows GOARCH=amd64 $(MAKE) build && mv bin/$(BINARY_NAME) bin/$(BINARY_NAME)-windows-amd64.exe

# Run tests
test:
	@echo "Running tests"
	go test -timeout $(TEST_TIMEOUT) -race -coverprofile=$(TEST_COVERAGE_PROFILE) ./...

# Run integration tests
test-integration:
	@echo "Running integration tests"
	go test -timeout $(TEST_TIMEOUT) -tags=integration ./pkg/testing/...

# Run benchmarks
bench:
	@echo "Running benchmarks"
	go test -bench=. -benchmem ./...

# Test coverage
coverage: test
	@echo "Generating coverage report"
	go tool cover -html=$(TEST_COVERAGE_PROFILE) -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Format code
fmt:
	@echo "Formatting code"
	go fmt ./...

# Vet code
vet:
	@echo "Vetting code"
	go vet ./...

# Lint code
lint:
	@echo "Linting code"
	golangci-lint run

# Download dependencies
deps:
	@echo "Downloading dependencies"
	go mod download
	go mod tidy

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts"
	rm -rf bin/
	rm -f $(TEST_COVERAGE_PROFILE) coverage.html

# Docker build
docker-build:
	@echo "Building Docker image: $(DOCKER_IMAGE)"
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(DOCKER_IMAGE) \
		.

# Docker push
docker-push: docker-build
	@echo "Pushing Docker image: $(DOCKER_IMAGE)"
	docker push $(DOCKER_IMAGE)

# Docker multi-arch build and push
docker-buildx:
	@echo "Building multi-arch Docker image: $(DOCKER_IMAGE)"
	docker buildx create --use --name benthos-coap-builder || true
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(DOCKER_IMAGE) \
		--push \
		.

# Run locally with example config
run-example:
	@echo "Running with example configuration"
	./bin/$(BINARY_NAME) -c examples/input-sensors.yaml

# Development setup
dev-setup:
	@echo "Setting up development environment"
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/goreleaser/goreleaser@latest
	
# Release (requires goreleaser)
release:
	@echo "Creating release"
	goreleaser release --clean

# Release snapshot (for testing)
release-snapshot:
	@echo "Creating snapshot release"
	goreleaser release --snapshot --clean

# Help
help:
	@echo "Available targets:"
	@echo "  build           - Build the binary"
	@echo "  build-all       - Build for multiple platforms"
	@echo "  test            - Run tests"
	@echo "  test-integration- Run integration tests"
	@echo "  bench           - Run benchmarks"
	@echo "  coverage        - Generate test coverage report"
	@echo "  fmt             - Format code"
	@echo "  vet             - Vet code"
	@echo "  lint            - Lint code"
	@echo "  deps            - Download dependencies"
	@echo "  clean           - Clean build artifacts"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-push     - Push Docker image"
	@echo "  docker-buildx   - Build and push multi-arch Docker image"
	@echo "  run-example     - Run with example configuration"
	@echo "  dev-setup       - Setup development environment"
	@echo "  release         - Create release"
	@echo "  release-snapshot- Create snapshot release"
	@echo "  help            - Show this help"

---

# Dockerfile
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build arguments
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-w -s -X main.Version=${VERSION} -X main.GitCommit=${GIT_COMMIT} -X main.BuildTime=${BUILD_TIME}" \
    -o benthos-coap \
    ./cmd

# Final stage
FROM alpine:3.18

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -s /bin/sh benthos

# Copy binary and set permissions
COPY --from=builder /build/benthos-coap /usr/local/bin/benthos-coap
RUN chmod +x /usr/local/bin/benthos-coap

# Copy example configurations
COPY --from=builder /build/examples /etc/benthos-coap/examples

# Set user
USER benthos

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD benthos-coap --version || exit 1

# Default command
ENTRYPOINT ["benthos-coap"]
CMD ["--help"]

---

# .dockerignore
# Git
.git
.gitignore

# Build artifacts
bin/
*.out
coverage.html

# Development files
.vscode/
.idea/
*.swp
*.swo
*~

# OS files
.DS_Store
Thumbs.db

# Documentation
*.md
docs/

# CI/CD
.github/
.gitlab-ci.yml

# Test files
*_test.go
testdata/

---

# docker-compose.yml
version: '3.8'

services:
  # Benthos CoAP Plugin
  benthos-coap:
    build: .
    container_name: benthos-coap
    volumes:
      - ./examples:/config
      - ./data:/data
    ports:
      - "4195:4195"  # Benthos HTTP server
    environment:
      - BENTHOS_LOG_LEVEL=INFO
    command: ["-c", "/config/input-sensors.yaml"]
    depends_on:
      - coap-server
      - kafka

  # Mock CoAP Server for testing
  coap-server:
    image: eclipse/californium:latest
    container_name: coap-test-server
    ports:
      - "5683:5683/udp"
      - "5684:5684/udp"  # DTLS
    environment:
      - COAP_PORT=5683
      - COAPS_PORT=5684

  # Kafka for output testing
  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  # Prometheus for metrics
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'

  # Grafana for visualization
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning

volumes:
  kafka-data:
  prometheus-data:
  grafana-data:

---

# .goreleaser.yaml
project_name: benthos-coap

before:
  hooks:
    - go mod tidy
    - go generate ./...

builds:
  - main: ./cmd
    id: benthos-coap
    binary: benthos-coap
    env:
      - CGO_ENABLED=0
    goos:
      - linux
      - darwin
      - windows
    goarch:
      - amd64
      - arm64
    ldflags:
      - -s -w
      - -X main.Version={{.Version}}
      - -X main.GitCommit={{.FullCommit}}
      - -X main.BuildTime={{.Date}}

archives:
  - id: benthos-coap
    builds:
      - benthos-coap
    name_template: "{{ .ProjectName }}_{{ .Version }}_{{ .Os }}_{{ .Arch }}"
    format_overrides:
      - goos: windows
        format: zip
    files:
      - README.md
      - LICENSE
      - examples/**/*

dockers:
  - image_templates:
      - "ghcr.io/your-org/benthos-coap:{{ .Version }}-amd64"
      - "ghcr.io/your-org/benthos-coap:latest-amd64"
    dockerfile: Dockerfile
    use: buildx
    build_flag_templates:
      - "--platform=linux/amd64"
      - "--build-arg=VERSION={{.Version}}"
      - "--build-arg=GIT_COMMIT={{.FullCommit}}"
      - "--build-arg=BUILD_TIME={{.Date}}"
  
  - image_templates:
      - "ghcr.io/your-org/benthos-coap:{{ .Version }}-arm64"
      - "ghcr.io/your-org/benthos-coap:latest-arm64"
    dockerfile: Dockerfile
    use: buildx
    build_flag_templates:
      - "--platform=linux/arm64"
      - "--build-arg=VERSION={{.Version}}"
      - "--build-arg=GIT_COMMIT={{.FullCommit}}"
      - "--build-arg=BUILD_TIME={{.Date}}"
    goarch: arm64

docker_manifests:
  - name_template: "ghcr.io/your-org/benthos-coap:{{ .Version }}"
    image_templates:
      - "ghcr.io/your-org/benthos-coap:{{ .Version }}-amd64"
      - "ghcr.io/your-org/benthos-coap:{{ .Version }}-arm64"
  
  - name_template: "ghcr.io/your-org/benthos-coap:latest"
    image_templates:
      - "ghcr.io/your-org/benthos-coap:latest-amd64"
      - "ghcr.io/your-org/benthos-coap:latest-arm64"

checksum:
  name_template: 'checksums.txt'

snapshot:
  name_template: "{{ incpatch .Version }}-next"

changelog:
  sort: asc
  filters:
    exclude:
      - '^docs:'
      - '^test:'
      - '^chore:'
      - '^ci:'

release:
  github:
    owner: your-org
    name: benthos-coap
  draft: false
  prerelease: auto

---

# scripts/install.sh
#!/bin/bash
set -e

# Benthos CoAP Plugin installer script

REPO="your-org/benthos-coap"
BINARY_NAME="benthos-coap"
INSTALL_DIR="/usr/local/bin"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case $ARCH in
    x86_64) ARCH="amd64" ;;
    arm64) ARCH="arm64" ;;
    aarch64) ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Get latest release
if [ -z "$VERSION" ]; then
    VERSION=$(curl -s "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
fi

if [ -z "$VERSION" ]; then
    echo "Failed to get latest version"
    exit 1
fi

echo "Installing $BINARY_NAME $VERSION for $OS/$ARCH"

# Download URL
DOWNLOAD_URL="https://github.com/$REPO/releases/download/$VERSION/${BINARY_NAME}_${VERSION}_${OS}_${ARCH}.tar.gz"

# Download and extract
TMP_DIR=$(mktemp -d)
cd "$TMP_DIR"

echo "Downloading from $DOWNLOAD_URL"
curl -sL "$DOWNLOAD_URL" | tar xz

# Install binary
sudo mv "$BINARY_NAME" "$INSTALL_DIR/"
sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"

# Clean up
cd -
rm -rf "$TMP_DIR"

echo "$BINARY_NAME installed successfully to $INSTALL_DIR"
echo "Run '$BINARY_NAME --help' to get started"