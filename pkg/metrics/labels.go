// pkg/metrics/labels.go
package metrics

// Labels provides consistent labeling for metrics
type Labels struct {
	Component string
	Protocol  string
	Endpoint  string
	Path      string
	Method    string
}

// NewLabels creates new metric labels
func NewLabels(component, protocol string) *Labels {
	return &Labels{
		Component: component,
		Protocol:  protocol,
	}
}

// WithEndpoint adds endpoint label
func (l *Labels) WithEndpoint(endpoint string) *Labels {
	l.Endpoint = endpoint
	return l
}

// WithPath adds path label
func (l *Labels) WithPath(path string) *Labels {
	l.Path = path
	return l
}

// WithMethod adds method label
func (l *Labels) WithMethod(method string) *Labels {
	l.Method = method
	return l
}

// ToMap converts labels to map for Benthos metrics
func (l *Labels) ToMap() map[string]string {
	result := make(map[string]string)

	if l.Component != "" {
		result["component"] = l.Component
	}
	if l.Protocol != "" {
		result["protocol"] = l.Protocol
	}
	if l.Endpoint != "" {
		result["endpoint"] = l.Endpoint
	}
	if l.Path != "" {
		result["path"] = l.Path
	}
	if l.Method != "" {
		result["method"] = l.Method
	}

	return result
}
