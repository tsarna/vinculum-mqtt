package subscriber

import (
	"context"
	"errors"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// captureSubscriber records the arguments passed to OnEvent.
type captureSubscriber struct {
	bus.BaseSubscriber
	topic  string
	msg    any
	fields map[string]string
	err    error
}

func (s *captureSubscriber) OnEvent(_ context.Context, topic string, msg any, fields map[string]string) error {
	s.topic = topic
	s.msg = msg
	s.fields = fields
	return s.err
}

// staticTopicFunc always returns the given vinculum topic.
func staticTopicFunc(topic string) VinculumTopicFunc {
	return func(_ string, _ map[string]string, _ any) (string, error) {
		return topic, nil
	}
}

// makePub builds a minimal *paho.Publish for testing.
func makePub(topic string, payload []byte) *paho.Publish {
	return &paho.Publish{Topic: topic, Payload: payload}
}

// makeSubscriber builds an MQTTSubscriber with a single subscription.
func makeSubscriber(pattern string, fn VinculumTopicFunc, target bus.Subscriber, handleRetained bool) *MQTTSubscriber {
	return &MQTTSubscriber{
		subscriptions: []TopicSubscription{
			{
				MQTTPattern:       pattern,
				BrokerTopic:       stripFieldNames(pattern),
				VinculumTopicFunc: fn,
			},
		},
		subscriber:     target,
		handleRetained: handleRetained,
		wireFormat:     wire.Auto,
	}
}

// ── deserialize (via wire format) ─────────────────────────────────────────────

func TestDeserialize_ValidJSONObject(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`{"a":1}`))
	require.NoError(t, err)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), m["a"])
}

func TestDeserialize_ValidJSONArray(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`[1,2,3]`))
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

func TestDeserialize_ValidJSONString(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`"hello"`))
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestDeserialize_InvalidJSON(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte("not-json"))
	require.NoError(t, err)
	// auto format falls back to string per spec
	assert.Equal(t, "not-json", result)
}

// ── userPropertiesToFields ────────────────────────────────────────────────────

func TestUserPropertiesToFields_Nil(t *testing.T) {
	assert.Nil(t, userPropertiesToFields(nil))
}

func TestUserPropertiesToFields_Empty(t *testing.T) {
	assert.Nil(t, userPropertiesToFields(&paho.PublishProperties{}))
}

func TestUserPropertiesToFields_Single(t *testing.T) {
	props := &paho.PublishProperties{
		User: paho.UserProperties{{Key: "region", Value: "eu"}},
	}
	assert.Equal(t, map[string]string{"region": "eu"}, userPropertiesToFields(props))
}

func TestUserPropertiesToFields_DuplicateKeyLastValueWins(t *testing.T) {
	props := &paho.PublishProperties{
		User: paho.UserProperties{
			{Key: "k", Value: "first"},
			{Key: "k", Value: "last"},
		},
	}
	result := userPropertiesToFields(props)
	assert.Equal(t, "last", result["k"])
}

// ── stripFieldNames ───────────────────────────────────────────────────────────

func TestStripFieldNames_NoFields(t *testing.T) {
	assert.Equal(t, "sensor/+/data", stripFieldNames("sensor/+/data"))
}

func TestStripFieldNames_WithFieldName(t *testing.T) {
	assert.Equal(t, "sensor/+/data", stripFieldNames("sensor/+deviceId/data"))
}

func TestStripFieldNames_MultipleFields(t *testing.T) {
	assert.Equal(t, "+/+/data", stripFieldNames("+type/+deviceId/data"))
}

func TestStripFieldNames_HashUnchanged(t *testing.T) {
	assert.Equal(t, "sensor/#", stripFieldNames("sensor/#"))
}

func TestStripFieldNames_NoWildcards(t *testing.T) {
	assert.Equal(t, "exact/topic", stripFieldNames("exact/topic"))
}

// ── findSubscription ──────────────────────────────────────────────────────────

func TestFindSubscription_ExactMatch(t *testing.T) {
	s := makeSubscriber("sensor/temp", nil, &captureSubscriber{}, true)
	sub, err := s.findSubscription("sensor/temp")
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", sub.MQTTPattern)
}

func TestFindSubscription_WildcardMatch(t *testing.T) {
	s := makeSubscriber("sensor/#", nil, &captureSubscriber{}, true)
	sub, err := s.findSubscription("sensor/temp/reading")
	require.NoError(t, err)
	assert.Equal(t, "sensor/#", sub.MQTTPattern)
}

func TestFindSubscription_FieldNamePattern(t *testing.T) {
	s := makeSubscriber("sensor/+deviceId/reading", nil, &captureSubscriber{}, true)
	sub, err := s.findSubscription("sensor/abc/reading")
	require.NoError(t, err)
	assert.Equal(t, "sensor/+deviceId/reading", sub.MQTTPattern)
}

func TestFindSubscription_NoMatch(t *testing.T) {
	s := makeSubscriber("sensor/#", nil, &captureSubscriber{}, true)
	_, err := s.findSubscription("status/online")
	assert.Error(t, err)
}

// ── BrokerSubscriptions ───────────────────────────────────────────────────────

func TestBrokerSubscriptions_NoSharedGroup(t *testing.T) {
	s, err := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: "sensor/+deviceId/data", QoS: 1}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	require.NoError(t, err)

	opts := s.BrokerSubscriptions()
	require.Len(t, opts, 1)
	assert.Equal(t, "sensor/+/data", opts[0].Topic)
	assert.Equal(t, byte(1), opts[0].QoS)
}

func TestBrokerSubscriptions_WithSharedGroup(t *testing.T) {
	s, err := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: "sensor/#", QoS: 0}).
		WithSubscriber(&captureSubscriber{}).
		WithSharedGroup("workers").
		Build()
	require.NoError(t, err)

	opts := s.BrokerSubscriptions()
	require.Len(t, opts, 1)
	assert.Equal(t, "$share/workers/sensor/#", opts[0].Topic)
}

func TestBrokerSubscriptions_MultipleSubscriptions(t *testing.T) {
	s, err := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: "sensor/#"}).
		WithSubscription(TopicSubscription{MQTTPattern: "status/#"}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	require.NoError(t, err)

	opts := s.BrokerSubscriptions()
	assert.Len(t, opts, 2)
}

// ── HandleMessage ─────────────────────────────────────────────────────────────

func TestHandleMessage_CorrectDispatch(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("sensor/+deviceId/reading", staticTopicFunc("mapped/topic"), target, true)

	pub := &paho.Publish{
		Topic:   "sensor/abc/reading",
		Payload: []byte(`{"temp":22.5}`),
		Properties: &paho.PublishProperties{
			User: paho.UserProperties{{Key: "region", Value: "eu"}},
		},
	}

	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)
	assert.Equal(t, "mapped/topic", target.topic)
	assert.Equal(t, map[string]any{"temp": 22.5}, target.msg)
	// Fields: user property "region" + extracted field "deviceId"
	assert.Equal(t, "eu", target.fields["region"])
	assert.Equal(t, "abc", target.fields["deviceId"])
}

func TestHandleMessage_VerbatimTopicWhenNoFunc(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("sensor/#", nil, target, true)

	err := s.HandleMessage(context.Background(), makePub("sensor/temp", nil))
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", target.topic)
}

func TestHandleMessage_VinculumTopicFuncEmptyUsesVerbatim(t *testing.T) {
	target := &captureSubscriber{}
	fn := func(_ string, _ map[string]string, _ any) (string, error) { return "", nil }
	s := makeSubscriber("sensor/#", fn, target, true)

	err := s.HandleMessage(context.Background(), makePub("sensor/temp", nil))
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", target.topic)
}

func TestHandleMessage_VinculumTopicFuncError(t *testing.T) {
	target := &captureSubscriber{}
	fn := func(_ string, _ map[string]string, _ any) (string, error) {
		return "", errors.New("bad expression")
	}
	s := makeSubscriber("sensor/#", fn, target, true)

	err := s.HandleMessage(context.Background(), makePub("sensor/temp", nil))
	assert.Error(t, err)
	assert.Empty(t, target.topic)
}

func TestHandleMessage_RetainedDroppedWhenDisabled(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("sensor/#", nil, target, false) // handleRetained=false

	pub := &paho.Publish{Topic: "sensor/temp", Payload: nil, Retain: true}
	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)
	assert.Empty(t, target.topic, "retained message should be dropped")
}

func TestHandleMessage_RetainedDeliveredWhenEnabled(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("sensor/#", nil, target, true) // handleRetained=true

	pub := &paho.Publish{Topic: "sensor/temp", Payload: []byte(`"val"`), Retain: true}
	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)
	assert.Equal(t, "sensor/temp", target.topic)
	assert.Equal(t, "true", target.fields["$retained"])
}

func TestHandleMessage_UserPropertiesInFields(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("test/#", nil, target, true)

	pub := &paho.Publish{
		Topic: "test/x",
		Properties: &paho.PublishProperties{
			User: paho.UserProperties{
				{Key: "a", Value: "1"},
				{Key: "b", Value: "2"},
			},
		},
	}
	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)
	assert.Equal(t, "1", target.fields["a"])
	assert.Equal(t, "2", target.fields["b"])
}

func TestHandleMessage_SubscriberError(t *testing.T) {
	target := &captureSubscriber{err: errors.New("downstream failure")}
	s := makeSubscriber("test/#", nil, target, true)

	err := s.HandleMessage(context.Background(), makePub("test/x", nil))
	assert.ErrorContains(t, err, "downstream failure")
}

func TestHandleMessage_NoMatch(t *testing.T) {
	target := &captureSubscriber{}
	s := makeSubscriber("sensor/#", nil, target, true)

	err := s.HandleMessage(context.Background(), makePub("status/online", nil))
	assert.Error(t, err)
	assert.Empty(t, target.topic)
}

// ── userPropertiesToFields trace filtering ────────────────────────────────────

func TestUserPropertiesToFields_FiltersTraceHeaders(t *testing.T) {
	props := &paho.PublishProperties{
		User: paho.UserProperties{
			{Key: "region", Value: "eu-west"},
			{Key: "traceparent", Value: "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"},
			{Key: "tracestate", Value: "vendor=abc"},
			{Key: "baggage", Value: "userId=42"},
		},
	}
	result := userPropertiesToFields(props)
	assert.Equal(t, map[string]string{"region": "eu-west"}, result,
		"trace headers should be filtered, business headers should remain")
}

func TestUserPropertiesToFields_OnlyTraceHeaders(t *testing.T) {
	props := &paho.PublishProperties{
		User: paho.UserProperties{
			{Key: "traceparent", Value: "00-abc-def-01"},
			{Key: "tracestate", Value: "k=v"},
		},
	}
	assert.Nil(t, userPropertiesToFields(props),
		"should return nil when only trace headers are present")
}

// ── tracing helpers ───────────────────────────────────────────────────────────

func setupTestTracer(t *testing.T) (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() {
		tp.Shutdown(context.Background()) //nolint:errcheck
	})
	return exporter, tp
}

// ── HandleMessage tracing ─────────────────────────────────────────────────────

func makeTracedSubscriber(pattern string, fn VinculumTopicFunc, target bus.Subscriber, tp *sdktrace.TracerProvider) *MQTTSubscriber {
	s, _ := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: pattern}).
		WithSubscriber(target).
		WithTracerProvider(tp).
		Build()
	s.subscriptions[0].VinculumTopicFunc = fn
	return s
}

func TestHandleMessage_CreatesProcessSpan(t *testing.T) {
	exporter, tp := setupTestTracer(t)

	target := &captureSubscriber{}
	s := makeTracedSubscriber("test/#", staticTopicFunc("a/b"), target, tp)

	err := s.HandleMessage(context.Background(), makePub("test/x", []byte(`{}`)))
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Equal(t, "process a/b", spans[0].Name)
}

func TestHandleMessage_LinksRemoteTraceContext(t *testing.T) {
	exporter, tp := setupTestTracer(t)

	target := &captureSubscriber{}
	s := makeTracedSubscriber("test/#", staticTopicFunc("a/b"), target, tp)

	remoteTraceID := "80e1afed08e019fc1110464cfa66635c"
	pub := &paho.Publish{
		Topic:   "test/x",
		Payload: []byte(`{}`),
		Properties: &paho.PublishProperties{
			User: paho.UserProperties{
				{Key: "traceparent", Value: "00-" + remoteTraceID + "-7a085853722dc6d2-01"},
			},
		},
	}

	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.NotEqual(t, remoteTraceID, spans[0].SpanContext.TraceID().String(),
		"vinculum processing span should be a new trace root, not a child of the remote trace")
	require.Len(t, spans[0].Links, 1, "expected one link to the remote producer span")
	assert.Equal(t, remoteTraceID, spans[0].Links[0].SpanContext.TraceID().String(),
		"link should point to the remote producer trace")
}

func TestHandleMessage_TraceHeadersNotInFields(t *testing.T) {
	_, tp := setupTestTracer(t)

	target := &captureSubscriber{}
	s := makeTracedSubscriber("test/#", nil, target, tp)

	pub := &paho.Publish{
		Topic:   "test/x",
		Payload: []byte(`{}`),
		Properties: &paho.PublishProperties{
			User: paho.UserProperties{
				{Key: "region", Value: "eu"},
				{Key: "traceparent", Value: "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"},
			},
		},
	}

	err := s.HandleMessage(context.Background(), pub)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"region": "eu"}, target.fields,
		"traceparent must not appear in the fields delivered to the subscriber")
}

func TestHandleMessage_SpanRecordsError(t *testing.T) {
	exporter, tp := setupTestTracer(t)

	target := &captureSubscriber{err: errors.New("downstream failure")}
	s := makeTracedSubscriber("test/#", staticTopicFunc("a/b"), target, tp)

	err := s.HandleMessage(context.Background(), makePub("test/x", []byte(`{}`)))
	assert.Error(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Len(t, spans[0].Events, 1, "expected one error event on the span")
}

// ── Builder validation ────────────────────────────────────────────────────────

func TestBuild_NoSubscriber(t *testing.T) {
	_, err := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: "test/#"}).
		Build()
	assert.Error(t, err)
}

func TestBuild_NoSubscriptions(t *testing.T) {
	_, err := NewSubscriber().
		WithSubscriber(&captureSubscriber{}).
		Build()
	assert.Error(t, err)
}

func TestBuild_BrokerTopicComputedAutomatically(t *testing.T) {
	s, err := NewSubscriber().
		WithSubscription(TopicSubscription{MQTTPattern: "sensor/+deviceId/data"}).
		WithSubscriber(&captureSubscriber{}).
		Build()
	require.NoError(t, err)
	assert.Equal(t, "sensor/+/data", s.subscriptions[0].BrokerTopic)
}
