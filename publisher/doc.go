// Package publisher provides MQTTPublisher, a bus.Subscriber that forwards
// vinculum events to an MQTT broker.
//
// # Usage
//
// Build a publisher using the fluent builder:
//
//	p, err := publisher.NewPublisher().
//	    WithDefaultQoS(1).
//	    WithTopicMapping(publisher.TopicMapping{
//	        Pattern: "alerts/#",
//	        QoS:     1,
//	        Retain:  true,
//	    }).
//	    Build()
//
// The publisher requires a publish function injected by client.MQTTClient
// after the connection is established:
//
//	p.SetPublishFunc(cm.Publish)
//
// # Topic mapping
//
// Mappings are matched in declaration order; the first match wins. When no
// mapping matches, DefaultTopicTransform applies:
//   - DefaultTopicVerbatim (default): publish to the vinculum topic verbatim
//   - DefaultTopicError: return an error from OnEvent
//   - DefaultTopicIgnore: silently drop the message
//
// # Payload serialization
//
//   - cty.Value: converted via go2cty2go.CtyToAny, then json.Marshal
//   - []byte: passed through unchanged
//   - other: json.Marshal
//
// vinculum fields are encoded as MQTT 5 user properties.
package publisher
