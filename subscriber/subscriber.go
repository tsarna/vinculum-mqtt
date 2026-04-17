package subscriber

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/amir-yaghoubi/mqttpattern"
	"github.com/eclipse/paho.golang/paho"
	"github.com/tsarna/vinculum-mqtt/carrier"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
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
	wireFormat     wire.WireFormat
	logger         *zap.Logger
	metrics        *SubscriberMetrics
	tracerProvider trace.TracerProvider
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

	var msg any
	if pub.Payload != nil {
		var deserErr error
		msg, deserErr = s.wireFormat.Deserialize(pub.Payload)
		if deserErr != nil {
			s.logger.Warn("mqtt subscriber: deserialize failed, passing raw bytes",
				zap.String("topic", pub.Topic),
				zap.Error(deserErr))
			msg = pub.Payload
		}
	}

	// Extract W3C trace context from MQTT 5 user properties before building
	// the fields map. The carrier gives the propagator read access to user
	// properties without modifying them.
	var rawProps paho.UserProperties
	if pub.Properties != nil {
		rawProps = pub.Properties.User
	}
	remoteCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier.New(rawProps))
	remoteSpanCtx := trace.SpanContextFromContext(remoteCtx)

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

	// Create a span covering the full vinculum processing time (topic
	// resolution, deserialization, and subscriber.OnEvent including action
	// evaluation). Per OTel messaging semantic conventions, consumer spans
	// should be new trace roots linked to the producer span rather than
	// children of it, correctly representing the async pub/sub boundary.
	tp := s.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("vinculum-mqtt/subscriber")
	spanOpts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("mqtt"),
			semconv.MessagingDestinationNameKey.String(pub.Topic),
			semconv.MessagingOperationTypeDeliver,
			semconv.MessagingOperationNameKey.String("process"),
		),
	}
	if remoteSpanCtx.IsValid() {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: remoteSpanCtx}))
	}
	ctx, span := tracer.Start(ctx, "process "+vinculumTopic, spanOpts...)
	defer span.End()

	start := time.Now()
	err = s.subscriber.OnEvent(ctx, vinculumTopic, msg, fields)
	s.metrics.RecordProcessDuration(ctx, pub.Topic, time.Since(start))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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


// traceHeaders is the set of W3C trace context keys injected by OTel
// propagators. These are filtered from the fields map so business metadata
// stays clean — the propagator has already extracted them into the context.
var traceHeaders = map[string]struct{}{
	"traceparent": {},
	"tracestate":  {},
	"baggage":     {},
}

// userPropertiesToFields converts MQTT 5 user properties to a string map,
// filtering out W3C trace context headers (traceparent, tracestate, baggage).
// Duplicate keys: last value wins. Returns nil when there are no
// non-trace properties.
func userPropertiesToFields(props *paho.PublishProperties) map[string]string {
	if props == nil || len(props.User) == 0 {
		return nil
	}
	m := make(map[string]string, len(props.User))
	for _, p := range props.User {
		if _, isTrace := traceHeaders[p.Key]; !isTrace {
			m[p.Key] = p.Value
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
