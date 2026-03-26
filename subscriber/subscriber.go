package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/amir-yaghoubi/mqttpattern"
	"github.com/eclipse/paho.golang/paho"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// VinculumTopicFunc resolves the vinculum topic for an inbound MQTT message.
// mqttTopic is the concrete topic the message arrived on; fields is populated
// from MQTT 5 user properties merged with any pattern-extracted field segments;
// msg is the deserialized payload. Return "" to use mqttTopic verbatim.
//
// Constructed by the config layer to avoid a circular import dependency.
type VinculumTopicFunc func(mqttTopic string, fields map[string]string, msg any) (string, error)

// TopicSubscription maps one MQTT topic pattern to a vinculum topic resolver
// and a QoS level for the broker subscription.
type TopicSubscription struct {
	// MQTTPattern is the full pattern including optional field names
	// (e.g. "sensor/+deviceId/data").
	MQTTPattern string

	// BrokerTopic is the subscription pattern sent to the broker — field names
	// stripped from + segments (e.g. "sensor/+/data"). Computed by the builder.
	BrokerTopic string

	// QoS is the MQTT quality-of-service level requested from the broker (0 or 1).
	QoS byte

	// VinculumTopicFunc resolves the vinculum topic per message.
	// nil means use the concrete MQTT topic verbatim.
	VinculumTopicFunc VinculumTopicFunc
}

// MQTTSubscriber receives inbound MQTT messages and dispatches them as vinculum
// events. It is a source, not a sink — it does NOT implement bus.Subscriber.
// Create via NewSubscriber().Build().
type MQTTSubscriber struct {
	subscriptions  []TopicSubscription
	subscriber     bus.Subscriber
	handleRetained bool
	sharedGroup    string
	logger         *zap.Logger
	metrics        *SubscriberMetrics
}

// HandleMessage is registered with the paho router by client.MQTTClient. It
// finds the matching subscription, deserializes the payload, builds the fields
// map from MQTT 5 user properties and pattern-extracted segments, resolves the
// vinculum topic, and calls subscriber.OnEvent.
func (s *MQTTSubscriber) HandleMessage(ctx context.Context, pub *paho.Publish) error {
	if pub.Retain && !s.handleRetained {
		return nil // silently drop retained messages when not configured to handle them
	}

	sub, err := s.findSubscription(pub.Topic)
	if err != nil {
		s.metrics.RecordError(ctx, pub.Topic)
		return err
	}

	msg := deserializePayload(pub.Payload)

	fields := userPropertiesToFields(pub.Properties)

	// Merge pattern-extracted fields; they take precedence over user properties.
	extracted := mqttpattern.Extract(sub.MQTTPattern, pub.Topic)
	if len(extracted) > 0 {
		if fields == nil {
			fields = make(map[string]string, len(extracted))
		}
		for k, v := range extracted {
			fields[k] = v
		}
	}

	if pub.Retain {
		if fields == nil {
			fields = make(map[string]string, 1)
		}
		fields["$retained"] = "true"
	}

	vinculumTopic := pub.Topic
	if sub.VinculumTopicFunc != nil {
		vinculumTopic, err = sub.VinculumTopicFunc(pub.Topic, fields, msg)
		if err != nil {
			s.metrics.RecordError(ctx, pub.Topic)
			return fmt.Errorf("mqtt subscriber: resolve vinculum topic for %q: %w", pub.Topic, err)
		}
		if vinculumTopic == "" {
			vinculumTopic = pub.Topic
		}
	}

	start := time.Now()
	err = s.subscriber.OnEvent(ctx, vinculumTopic, msg, fields)
	s.metrics.RecordProcessDuration(ctx, pub.Topic, time.Since(start))
	if err != nil {
		s.metrics.RecordError(ctx, pub.Topic)
		return err
	}
	s.metrics.RecordReceived(ctx, pub.Topic)
	return nil
}

// BrokerSubscriptions returns the paho.SubscribeOptions for all subscriptions.
// When a shared group is configured, each topic is prefixed with
// "$share/<group>/".
func (s *MQTTSubscriber) BrokerSubscriptions() []paho.SubscribeOptions {
	opts := make([]paho.SubscribeOptions, len(s.subscriptions))
	for i, sub := range s.subscriptions {
		topic := sub.BrokerTopic
		if s.sharedGroup != "" {
			topic = "$share/" + s.sharedGroup + "/" + topic
		}
		opts[i] = paho.SubscribeOptions{
			Topic: topic,
			QoS:   sub.QoS,
		}
	}
	return opts
}

// findSubscription returns the first subscription whose MQTTPattern matches
// the concrete MQTT topic using mqttpattern.Matches.
func (s *MQTTSubscriber) findSubscription(mqttTopic string) (*TopicSubscription, error) {
	for i := range s.subscriptions {
		if mqttpattern.Matches(s.subscriptions[i].MQTTPattern, mqttTopic) {
			return &s.subscriptions[i], nil
		}
	}
	return nil, fmt.Errorf("mqtt subscriber: no subscription matched topic %q", mqttTopic)
}

// stripFieldNames converts a pattern with named + segments to a standard MQTT
// wildcard pattern suitable for broker subscriptions.
// "sensor/+deviceId/data" → "sensor/+/data"
func stripFieldNames(pattern string) string {
	segments := strings.Split(pattern, "/")
	for i, seg := range segments {
		if len(seg) > 1 && seg[0] == '+' {
			segments[i] = "+"
		}
	}
	return strings.Join(segments, "/")
}

// deserializePayload converts an MQTT message payload to a Go value.
// Valid JSON is unmarshalled to any (map/slice/scalar).
// Invalid or nil JSON is returned as []byte (or nil).
func deserializePayload(payload []byte) any {
	if payload == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return payload
	}
	return v
}

// userPropertiesToFields converts MQTT 5 user properties to a string map.
// Duplicate keys: last value wins. Returns nil when there are no properties.
func userPropertiesToFields(props *paho.PublishProperties) map[string]string {
	if props == nil || len(props.User) == 0 {
		return nil
	}
	m := make(map[string]string, len(props.User))
	for _, p := range props.User {
		m[p.Key] = p.Value
	}
	return m
}
