# scripts/dev-setup.sh
#!/bin/bash
set -e

echo "Setting up development environment for Benthos CoAP Plugin..."

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed. Please install Go 1.20 or later."
    exit 1
fi

# Check Go version
GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
REQUIRED_VERSION="1.20"

if ! printf '%s\n' "$REQUIRED_VERSION" "$GO_VERSION" | sort -V -C; then
    echo "❌ Go version $GO_VERSION is too old. Please upgrade to Go $REQUIRED_VERSION or later."
    exit 1
fi

echo "✅ Go version $GO_VERSION is compatible"

# Install development tools
echo "📦 Installing development tools..."

# golangci-lint
if ! command -v golangci-lint &> /dev/null; then
    echo "Installing golangci-lint..."
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
else
    echo "✅ golangci-lint already installed"
fi

# goreleaser
if ! command -v goreleaser &> /dev/null; then
    echo "Installing goreleaser..."
    go install github.com/goreleaser/goreleaser@latest
else
    echo "✅ goreleaser already installed"
fi

# Air for live reloading during development
if ! command -v air &> /dev/null; then
    echo "Installing air..."
    go install github.com/cosmtrek/air@latest
else
    echo "✅ air already installed"
fi

# Install project dependencies
echo "📦 Installing project dependencies..."
go mod download
go mod tidy

# Setup git hooks
echo "🔧 Setting up git hooks..."
mkdir -p .git/hooks

cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
echo "Running pre-commit checks..."

# Format code
echo "Formatting code..."
go fmt ./...

# Run linter
echo "Running linter..."
golangci-lint run

# Run tests
echo "Running tests..."
go test -race ./...

echo "✅ Pre-commit checks passed"
EOF

chmod +x .git/hooks/pre-commit

# Create air configuration for live reloading
cat > .air.toml << 'EOF'
root = "."
testdata_dir = "testdata"
tmp_dir = "tmp"

[build]
args_bin = ["-c", "examples/input-sensors.yaml"]
bin = "./tmp/main"
cmd = "go build -o ./tmp/main ./cmd"
delay = 1000
exclude_dir = ["assets", "tmp", "vendor", "testdata"]
exclude_file = []
exclude_regex = ["_test.go"]
exclude_unchanged = false
follow_symlink = false
full_bin = ""
include_dir = []
include_ext = ["go", "tpl", "tmpl", "html", "yaml", "yml"]
kill_delay = "0s"
log = "build-errors.log"
send_interrupt = false
stop_on_root = false

[color]
app = ""
build = "yellow"
main = "magenta"
runner = "green"
watcher = "cyan"

[log]
time = false

[misc]
clean_on_exit = false

[screen]
clear_on_rebuild = false
EOF

echo "✅ Development environment setup complete!"
echo ""
echo "Available commands:"
echo "  make build        - Build the binary"
echo "  make test         - Run tests"
echo "  make lint         - Run linter"
echo "  air               - Start development server with live reload"
echo "  make run-example  - Run with example configuration"

---
