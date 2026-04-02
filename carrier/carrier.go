// Package carrier implements propagation.TextMapCarrier over MQTT 5 user
// properties. It is used by the subscriber and publisher to extract and inject
// W3C trace context headers without importing the full propagation package.
package carrier

import "github.com/eclipse/paho.golang/paho"

// Carrier implements propagation.TextMapCarrier backed by a
// paho.UserProperties slice. The zero value is ready to use (empty).
type Carrier struct {
	props paho.UserProperties
}

// New creates a Carrier pre-populated from the given slice.
// Pass nil for an empty carrier.
func New(props paho.UserProperties) *Carrier {
	return &Carrier{props: props}
}

// Get returns the value of the first user property whose key matches key,
// or "" if not found.
func (c *Carrier) Get(key string) string {
	for _, p := range c.props {
		if p.Key == key {
			return p.Value
		}
	}
	return ""
}

// Set updates the first user property with the given key in place, or appends
// a new entry if no matching key exists.
func (c *Carrier) Set(key, val string) {
	for i, p := range c.props {
		if p.Key == key {
			c.props[i].Value = val
			return
		}
	}
	c.props = append(c.props, paho.UserProperty{Key: key, Value: val})
}

// Keys returns all user property keys in order.
func (c *Carrier) Keys() []string {
	keys := make([]string, len(c.props))
	for i, p := range c.props {
		keys[i] = p.Key
	}
	return keys
}

// UserProperties returns the underlying user property slice, including any
// values added via Set.
func (c *Carrier) UserProperties() paho.UserProperties {
	return c.props
}
