# Testing Strategy for Connection Package

## Current Approach

The connection package uses a hybrid testing strategy:

### 1. Pure Unit Tests
- `factory_test.go` - Tests factory creation and configuration validation
- `factory_only_test.go` - Tests the factory pattern implementation
- These tests don't require Benthos framework and test pure logic

### 2. Integration Tests (with Benthos)
- `connection_integration_test.go` - Tests that use `service.MockResources()`
- These tests require the full Benthos framework but test real interactions

### 3. Interface-based Testing
- `interfaces.go` - Provides abstraction layer for metrics
- Allows testing without direct Benthos dependencies

## Running Tests

```bash
# Run only pure unit tests (fast, no dependencies)
go test ./pkg/connection/ -run "TestFactory|TestDTLS|TestTCPTLS|TestCreate"

# Run integration tests (requires Benthos mocks)
go test ./pkg/connection/ -run "TestConnection.*Benthos"

# Run all tests
go test ./pkg/connection/ -v
```

## Future Improvements

1. **Refactor metrics usage**: Use the `MetricRecorder` interface throughout the codebase
2. **Mock CoAP server**: Create a proper mock CoAP server for testing connections
3. **Separate test packages**: Use `connection_test` package for black-box testing

## Known Issues

- Direct usage of `service.MetricCounter` makes mocking difficult
- Logger initialization needs proper nil checks
- Some tests require actual network connections which may fail in CI