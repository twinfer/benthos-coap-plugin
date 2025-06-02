// pkg/metrics/registry.go
package metrics

import (
	"fmt"
	"sync"
)

// Registry manages metric instances across components
type Registry struct {
	managers map[string]*Manager
	mu       sync.RWMutex
}

var (
	globalRegistry = &Registry{
		managers: make(map[string]*Manager),
	}
)

// GetGlobalRegistry returns the global metrics registry
func GetGlobalRegistry() *Registry {
	return globalRegistry
}

// Register adds a metrics manager to the registry
func (r *Registry) Register(name string, manager *Manager) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.managers[name]; exists {
		return fmt.Errorf("metrics manager %s already registered", name)
	}

	r.managers[name] = manager
	return nil
}

// Get retrieves a metrics manager by name
func (r *Registry) Get(name string) (*Manager, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	manager, exists := r.managers[name]
	return manager, exists
}

// Unregister removes a metrics manager from the registry
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.managers, name)
}

// List returns all registered manager names
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.managers))
	for name := range r.managers {
		names = append(names, name)
	}
	return names
}
