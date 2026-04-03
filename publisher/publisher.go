package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/amir-yaghoubi/mqttpattern"
	"github.com/eclipse/paho.golang/paho"
	"github.com/tsarna/go2cty2go"
	"github.com/tsarna/vinculum-mqtt/carrier"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/zclconf/go-cty/cty"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// PublishFunc is the function signature for publishing a message over MQTT.
// It matches autopaho.ConnectionManager.Publish and is injected by client.MQTTClient
// after the connection is established, keeping this package free of autopaho imports.
type PublishFunc func(ctx context.Context, pub *paho.Publish) (*paho.PublishResponse, error)

// DefaultTopicTransform controls fallback behaviour when no topic_mapping matches.
type DefaultTopicTransform int

const (
	// DefaultTopicVerbatim uses the vinculum topic as the MQTT topic (default).
	DefaultTopicVerbatim DefaultTopicTransform = iota

	// DefaultTopicError returns an error for unmatched topics.
	DefaultTopicError

	// DefaultTopicIgnore silently drops unmatched events.
	DefaultTopicIgnore
)

// MQTTTopicFunc resolves the MQTT topic for an outbound message matched by a
// TopicMapping. The topic and fields arguments reflect the vinculum event;
// extracted pattern fields are merged into fields before this call.
// Return "" to use the vinculum topic verbatim.
type MQTTTopicFunc func(topic string, msg any, fields map[string]string) (string, error)

// TopicMapping maps a vinculum MQTT-style topic pattern to an MQTT topic, QoS,
// and retain flag.
type TopicMapping struct {
	// Pattern is an MQTT-style topic pattern (supports + and # wildcards, with
	// optional field names e.g. "+deviceId").
	Pattern string

	// MQTTTopicFunc resolves the MQTT topic per message. nil means use the
	// vinculum topic verbatim.
	MQTTTopicFunc MQTTTopicFunc

	// QoS is the MQTT quality-of-service level for this mapping (0 or 1).
	QoS byte

	// Retain indicates whether the broker should retain this message.
	Retain bool
}

// MQTTPublisher implements bus.Subscriber and publishes received vinculum events
// to an MQTT broker. Create via NewPublisher().Build().
//
// The publisher starts in a "disconnected" state. Call SetPublishFunc to inject
// the MQTT publish function (done automatically by client.MQTTClient.Start).
// OnEvent returns an error if called before SetPublishFunc.
type MQTTPublisher struct {
	bus.BaseSubscriber
	mu             sync.RWMutex
	publishFunc    PublishFunc
	topicMappings  []TopicMapping
	defaultXform   DefaultTopicTransform
	defaultQoS     byte
	defaultRetain  bool
	logger         *zap.Logger
	metrics        *PublisherMetrics
	tracerProvider trace.TracerProvider
}

// SetPublishFunc injects the MQTT publish function. Thread-safe. Called by
// client.MQTTClient.Start after the autopaho connection is established.
func (p *MQTTPublisher) SetPublishFunc(fn PublishFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishFunc = fn
}

// OnEvent implements bus.Subscriber. It resolves the MQTT topic, QoS, and
// retain flag from the configured topic_mappings, serializes the payload to
// JSON (or passes []byte through unchanged), converts fields to MQTT 5 user
// properties, and publishes via the injected PublishFunc.
func (p *MQTTPublisher) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	p.mu.RLock()
	fn := p.publishFunc
	p.mu.RUnlock()

	if fn == nil {
		return fmt.Errorf("mqtt publisher: not yet connected")
	}

	mqttTopic, qos, retain, err := p.resolveMapping(topic, msg, fields)
	if err != nil {
		p.metrics.RecordError(ctx, topic)
		return err
	}
	if mqttTopic == "" {
		return nil // DefaultTopicIgnore — silently drop
	}

	payload, err := serializePayload(msg)
	if err != nil {
		p.metrics.RecordError(ctx, mqttTopic)
		return fmt.Errorf("mqtt publisher: serialize payload: %w", err)
	}

	pub := &paho.Publish{
		Topic:   mqttTopic,
		QoS:     qos,
		Retain:  retain,
		Payload: payload,
	}

	// Build user properties from fields, then inject the current span's trace
	// context so downstream consumers can continue the trace. The carrier takes
	// ownership of the userProps slice and may append to it.
	c := carrier.New(fieldsToUserProperties(fields))
	otel.GetTextMapPropagator().Inject(ctx, c)
	if up := c.UserProperties(); len(up) > 0 {
		pub.Properties = &paho.PublishProperties{User: up}
	}

	// Span covers the actual broker publish call.
	tp := p.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("vinculum-mqtt/publisher")
	ctx, span := tracer.Start(ctx, "send "+mqttTopic,
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("mqtt"),
			semconv.MessagingDestinationNameKey.String(mqttTopic),
			semconv.MessagingOperationTypePublish,
			semconv.MessagingOperationNameKey.String("send"),
		),
	)
	defer span.End()

	start := time.Now()
	_, err = fn(ctx, pub)
	elapsed := time.Since(start)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.metrics.RecordError(ctx, mqttTopic)
		return fmt.Errorf("mqtt publisher: publish to %q: %w", mqttTopic, err)
	}

	p.metrics.RecordSent(ctx, mqttTopic)
	p.metrics.RecordPublishDuration(ctx, mqttTopic, elapsed)
	return nil
}

// resolveMapping iterates topic_mappings in order (first match wins) and
// returns the MQTT topic, QoS, and retain flag. Falls back to defaultXform
// when no mapping matches.
func (p *MQTTPublisher) resolveMapping(topic string, msg any, fields map[string]string) (mqttTopic string, qos byte, retain bool, err error) {
	for _, m := range p.topicMappings {
		if !mqttpattern.Matches(m.Pattern, topic) {
			continue
		}

		// Merge pattern-extracted fields with provided fields.
		// Pattern-extracted values take precedence.
		extracted := mqttpattern.Extract(m.Pattern, topic)
		mergedFields := fields
		if len(extracted) > 0 {
			mergedFields = make(map[string]string, len(fields)+len(extracted))
			for k, v := range fields {
				mergedFields[k] = v
			}
			for k, v := range extracted {
				mergedFields[k] = v
			}
		}

		mqttTopic = topic
		if m.MQTTTopicFunc != nil {
			mqttTopic, err = m.MQTTTopicFunc(topic, msg, mergedFields)
			if err != nil {
				return "", 0, false, fmt.Errorf("mqtt publisher: resolve topic for %q: %w", topic, err)
			}
			if mqttTopic == "" {
				mqttTopic = topic
			}
		}
		return mqttTopic, m.QoS, m.Retain, nil
	}

	// No mapping matched.
	switch p.defaultXform {
	case DefaultTopicIgnore:
		return "", 0, false, nil
	case DefaultTopicError:
		return "", 0, false, fmt.Errorf("mqtt publisher: no topic mapping matched for topic %q and default_topic_transform is error", topic)
	default: // DefaultTopicVerbatim
		return topic, p.defaultQoS, p.defaultRetain, nil
	}
}

// serializePayload converts a vinculum message payload to []byte for the
// MQTT message payload.
//
//   - cty.Value  → go2cty2go.CtyToAny() → json.Marshal
//   - []byte     → pass through unchanged
//   - nil        → nil
//   - anything else → json.Marshal
func serializePayload(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	if val, ok := msg.(cty.Value); ok {
		var err error
		msg, err = go2cty2go.CtyToAny(val)
		if err != nil {
			return nil, fmt.Errorf("cty conversion: %w", err)
		}
	}

	if b, ok := msg.([]byte); ok {
		return b, nil
	}

	return json.Marshal(msg)
}

// fieldsToUserProperties converts a vinculum fields map to MQTT 5 user
// properties. Returns nil for an empty or nil map.
func fieldsToUserProperties(fields map[string]string) paho.UserProperties {
	if len(fields) == 0 {
		return nil
	}
	props := make(paho.UserProperties, 0, len(fields))
	for k, v := range fields {
		props = append(props, paho.UserProperty{Key: k, Value: v})
	}
	return props
}

// ensure MQTTPublisher implements bus.Subscriber
var _ bus.Subscriber = (*MQTTPublisher)(nil)
