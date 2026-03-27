// Package subscriber provides MQTTSubscriber, which receives MQTT messages and
// dispatches them as vinculum events via a bus.Subscriber.
//
// # Usage
//
// Build a subscriber using the fluent builder:
//
//	s, err := subscriber.NewSubscriber().
//	    WithSubscription(subscriber.TopicSubscription{
//	        MQTTPattern: "sensors/+deviceId/data",
//	        QoS:         1,
//	    }).
//	    WithSubscriber(myBus).
//	    Build()
//
// Register the subscriber with client.MQTTClient before calling Start:
//
//	mqttClient.AddSubscriber(s)
//
// # Topic patterns
//
// MQTTPattern may use standard MQTT wildcards (+, #) with optional field names:
//
//	"sensor/+/data"           — standard MQTT wildcard
//	"sensor/+deviceId/data"   — extracts "deviceId" from the matched segment
//
// The broker subscription always uses the plain wildcard (field names are
// stripped). Field extraction populates the vinculum fields map.
//
// # Message deserialization
//
//   - Valid JSON bytes: unmarshalled to any (map, slice, scalar)
//   - Non-JSON bytes: passed as []byte
//
// MQTT 5 user properties become vinculum fields (last value wins for duplicates).
// Retained messages add fields["$retained"] = "true" when handle_retained is true.
package subscriber
