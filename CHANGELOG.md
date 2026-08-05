# Changelog

## Unreleased

## v0.11.0 (2026-08-05)

### Added

- **`ClientConfig.MaxReconnectAttempts`** bounds how many consecutive failed attempts to
  re-establish a lost connection are made before the client gives up. Zero or negative
  reconnects forever, which is both the default and the behaviour before this field
  existed, so upgrading changes nothing until you set it.

  Zero meaning *unlimited* rather than *never reconnect* is deliberate: it makes the new
  field's zero value the pre-existing behaviour, and it is what `vinculum-bus`'s
  `AutoReconnector` already means by the same number. "Do not reconnect at all" is
  therefore not expressible here, as it is not there. `vinculum-rabbitmq` v0.4.0 adds the
  same field with the same semantics.

  Two properties worth knowing:

  - The count is of **consecutive** failures and resets on every successful connection, so
    the limit bounds a single outage rather than the lifetime of the client.
  - It governs **reconnection only**. The initial connection made by `Start` is retried
    indefinitely whatever this says, because a broker that is not listening yet at process
    start is an ordinary situation rather than one to give up on.

  Giving up is terminal and quiet, mirroring `AutoReconnector`: an error is logged, the
  connection manager shuts down, and `Done` is closed. Nothing restarts it, and the process
  keeps running.

  Enforcing the limit needs more than a callback return value. autopaho's
  `establishServerConnection` retries until its context is cancelled, and the one callback
  that returns a `bool` — `OnConnectionDown` — is consulted once per dropped connection
  rather than once per failed attempt, so it can refuse to reconnect at all but cannot end
  a reconnect cycle already under way. The limit is therefore enforced by counting failures
  in `OnConnectError` and cancelling the context the connection manager was built with.
  `Start` now derives that context from the caller's rather than passing the caller's
  straight through; cancelling the caller's context still works exactly as before.

- **Failed connection attempts are now logged** (`OnConnectError`, at warn level). The
  reconnect path previously had no diagnostics at all. During an outage this is one line
  per backoff interval.

## v0.10.0 (2026-08-02)

### Changed

- **BREAKING: the decode-error hook's MQTT topic is keyed `mqtt_topic`, not `topic`.**
  `topic` is reserved by `wire.DecodeError`'s own `Topic` field, and a consumer is
  expected to drop a colliding `Attrs` key rather than let a client shadow a fixed
  field. Vinculum does exactly that, so this key never reached a config at all; a
  consumer reading `e.Attrs` directly did see it, which is what makes the rename
  breaking for them.

  No information was lost either way — the dropped value duplicated `Topic` — but the
  key was unusable through any consumer that honours the reserved set, and every other
  client names its transport identifier after the transport (`routing_key`, `stream`,
  `entry_id`). `mqtt_topic` is also what Vinculum already calls this concept in its own
  MQTT client config, so a hook reading `ctx.mqtt_topic` matches the surrounding
  vocabulary.

  The two carry the same string today only because `Topic` falls back to the transport
  name when the vinculum topic cannot be computed without the payload. Naming them apart
  keeps a consumer correct if that fallback ever improves.

  Consumers reading `e.Attrs["topic"]` should read `e.Attrs["mqtt_topic"]`, or
  `e.Topic` if what they wanted was the vinculum topic.

### Added

- Requires `vinculum-wire` v0.5.0 for `wire.IsReservedAttr`, which the subscriber's
  tests now assert every `Attrs` key against — so a key that would be dropped by a
  consumer fails here instead of vanishing silently downstream.

## v0.9.0 (2026-07-19)

### Changed

- **BREAKING: deserialize failures are no longer swallowed.** `MQTTSubscriber.HandleMessage`
  used to log a warning and pass the **raw bytes** through as the message payload when the
  configured wire format failed to decode. That happened even when the caller explicitly
  configured `wire.JSON`, so there was no way to say "messages on this topic must be JSON".
  A decode failure is now fatal to the message: `HandleMessage` returns an error and the
  payload never reaches `subscriber.OnEvent`.

  MQTT has no negative acknowledgement, so the message is simply dropped — nothing
  accumulates and nothing is redelivered.

  Callers wanting best-effort decoding should use `wire.Auto`, which never fails (it yields
  a `string` for anything it can't parse as JSON). Note that is not an exact replacement:
  the old fallback produced `[]byte`, so a subscriber that type-switches on `[]byte` must
  be adjusted.

- **BREAKING: `SubscriberMetrics.RecordError` takes an `errType` argument** —
  `RecordError(ctx, topic, errType)` — recorded as the `error.type` attribute. Existing
  call sites pass `"subscription"`, `"vinculum_topic"`, or `"subscriber"`. Passing an
  empty `errType` omits the attribute.

- Requires `github.com/tsarna/vinculum-wire` v0.3.0 for the `DecodeError` /
  `DecodeErrorHook` types.

### Added

- `WithDecodeErrorHook(wire.DecodeErrorHook)` on the subscriber builder. The hook observes
  a decode failure — it receives the raw payload, the error, the format name, and the MQTT
  topic — but cannot suppress it: `HandleMessage` returns an error either way. nil (the
  default) means no observer.

- Deserialize failures are recorded on the error counter with
  `error.type = "deserialize"`.

## v0.8.1 (2026-06-26)

### Fixed

- **Inbound baggage now reaches `subscriber.OnEvent`.** The subscriber extracted
  W3C baggage from MQTT 5 user properties but only used it to link the producer
  span; the baggage was not carried onto the context passed to `OnEvent`, so
  consumers could not read it. It is now copied onto the processing context
  (the consumer span remains a new root linked to the producer — only baggage
  rides along, not the span parent).

## v0.8.0 (2026-05-27)

### Changed

- Changed license to Apache-2.0

## v0.7.2 (2026-04-23)

### Changed

- **Topic matching routes through `vinculum-bus/topicmatch`** — publisher routing and subscriber pattern matching now honor MQTT 5.0 §4.7.2: filters starting with `+` or `#` no longer match reserved `$`-prefixed topics. Exact and `$`-prefixed patterns are unaffected. Requires vinculum-bus v0.12.0.

## v0.7.1 (2026-04-18)

### Added

- **`vinculum.client.name` metric attribute** — all client, publisher, and subscriber metrics now carry a `vinculum.client.name` attribute identifying the vinculum client block. Publishers and subscribers accept `WithClientName(name)` on their builders; the client accepts `ClientName` in `ClientConfig`.

## v0.7.0 (2026-04-17)

### Added

- **Pluggable wire format support** — publisher and subscriber builders now accept `WithWireFormat(wire.WireFormat)` or `WithWireFormatName(name)` to control payload serialization/deserialization. Built-in formats: `auto` (default), `json`, `string`, `bytes`. The default `auto` preserves backward compatibility. Depends on `github.com/tsarna/vinculum-wire` v0.1.0.

### Changed

- **Strings serialize verbatim in auto mode** — the `auto` wire format passes strings through unchanged (not JSON-encoded). Previously, strings were JSON-encoded with quotes. Use `wire_format = "json"` for the old behavior.

### Removed

- **Inline `serializePayload` / `deserializePayload` functions** — replaced by the shared `vinculum-wire` module.
- **`go2cty2go` and `go-cty` dependencies** — cty conversion now handled by vinculum's `CtyWireFormat` decorator at the config layer.

## v0.6.0 (2026-04-08)

### Changed

- **OTel metrics replaces o11y.MetricsProvider abstraction** — client, publisher, and subscriber now accept `metric.MeterProvider` directly via `WithMeterProvider()` or `MeterProvider` config field (replacing `WithMetricsProvider(o11y.MetricsProvider)` / `MetricsProvider` field). Metric names follow OTel semantic conventions: `messaging.client.sent.messages`, `messaging.client.consumed.messages`, `messaging.client.operation.duration`, `messaging.process.duration` where applicable; `mqtt.client.connected`, `mqtt.client.reconnections`, `mqtt.publisher.errors`, `mqtt.subscriber.errors` for MQTT-specific metrics. All metrics carry `messaging.system=mqtt` and `messaging.destination.name` attributes. Requires vinculum-bus v0.11.0.

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
