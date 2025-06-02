// cmd/main.go
package main

import (
	"context"
	"log"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import Benthos core components
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	// Import our CoAP plugin components
	_ "github.com/twinfer/benthos-coap-plugin/pkg/input"
	_ "github.com/twinfer/benthos-coap-plugin/pkg/output"
)

// Version information
var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildTime = "unknown"
)

func main() {
	log.Printf("Starting Benthos CoAP Plugin v%s (commit: %s, built: %s)", Version, GitCommit, BuildTime)
	service.RunCLI(context.Background())
}
