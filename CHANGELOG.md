# Changelog

## [Unreleased]

## v0.5.0 (2026-04-03)

### Added

- **OTel span kinds** — publisher spans use `SpanKindProducer` and subscriber spans use `SpanKindConsumer`, enabling tracing backends to correctly classify spans and render messaging topology views.

## v0.4.0 (2026-04-03)

### Added

- **OTel messaging semantic convention attributes** — both publisher and subscriber spans now carry `messaging.system`, `messaging.destination.name`, `messaging.operation.type`, and `messaging.operation.name` attributes per the OTel messaging semantic conventions (`semconv/v1.26.0`).

## v0.3.0 (2026-04-03)

### Changed

- **OTel span links for MQTT subscriber traces** — the `process <topic>` span is now created as a new trace root with a link to the remote producer span, following the [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/messaging/) recommendation for async pub/sub boundaries. Previously the subscriber span was a child of the producer's trace.

## v0.2.0 (2026-04-02)

### Added

- **Distributed tracing via W3C TraceContext over MQTT 5 user properties** — bidirectional trace context propagation using `go.opentelemetry.io/otel` and the global `TextMapPropagator`:
  - **`carrier` package** — new `propagation.TextMapCarrier` implementation backed by `paho.UserProperties`, used by both subscriber and publisher.
  - **Subscriber**: extracts `traceparent`/`tracestate`/`baggage` from inbound message user properties into the context before processing. Creates a `process <topic>` child span that wraps the full vinculum processing time including `subscriber.OnEvent`. W3C trace headers are filtered from the `fields` map delivered to VCL actions so business metadata stays clean.
  - **Publisher**: injects the current span's trace context into outgoing message user properties so downstream consumers can continue the trace. Creates a `send <topic>` span around the broker publish call.

## v0.1.0 (2026-03-26)

Initial release.

### Added

- `publisher` package: `MQTTPublisher` implementing `bus.Subscriber`, forwarding vinculum events to MQTT.
  - Per-pattern topic mapping with optional topic rename, QoS, and retain overrides.
  - `DefaultTopicTransform`: verbatim (default), error, or ignore for unmapped topics.
  - Payload serialization: `cty.Value` → JSON, `[]byte` passthrough, other → JSON.
  - Fields → MQTT 5 user properties.
  - Metrics: messages sent, errors, publish duration.

- `subscriber` package: `MQTTSubscriber` routing inbound MQTT messages to vinculum events.
  - Named wildcard field extraction (`+deviceId` → `fields["deviceId"]`).
  - Optional `VinculumTopicFunc` for dynamic vinculum topic mapping.
  - Retained message handling with `handle_retained` flag and `$retained` field.
  - MQTT 5 user properties → vinculum fields (last-value-wins for duplicates).
  - Shared subscription support via `$share/<group>/<topic>` prefix.
  - Metrics: messages received, errors, process duration.

- `client` package: `MQTTClient` wrapping `autopaho.ConnectionManager`.
  - Single shared connection for all publishers and subscribers.
  - `Start()` blocks until first connection and subscriptions are registered.
  - Re-subscribes automatically on every reconnect.
  - Configurable reconnect backoff, TLS, credentials, and LWT.
  - `on_connect` / `on_disconnect` lifecycle callbacks.
  - Metrics: connected gauge, reconnect counter.
