package client

import (
	"sync"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/log"
	"github.com/tsarna/vinculum-bus/topicmatch"
)

// A router that routes to recipients rather than to topics.
//
// paho.StandardRouter keys handlers by topic filter and, for each inbound
// message, calls the handlers of *every* matching filter. That is right for
// its own model — one handler per topic, registered independently — and wrong
// for ours, where one subscriber declares several filters and wants the
// message once. A subscriber declaring both "sensors/#" and "sensors/+/temp"
// had every sensors/<x>/temp message delivered twice, which nothing in the
// vinculum configuration reads as asking for.
//
// What the subscriber does with the overlap is a separate question this does
// not answer: findSubscription is first-match-wins over declaration order, so
// "sensors/#" listed first still shadows the specific filter's own vinculum
// topic and extractions. That is unchanged and deliberate. What this fix
// changes is the cost of getting it wrong — one wasted subscription rather than
// a duplicate delivery — and naming it belongs at config load, where the whole
// subscription list is known: tsarna/vinculum#239.
//
// The union of a subscriber's filters cannot be expressed as one filter, so
// registering one route per subscriber is not available. What is available is
// to make the recipient, not the filter, the thing the router iterates: for
// each entry, does any of its patterns match, and if so call it once.
//
// Two consequences fall out rather than being designed in:
//
//   - The $share/<group>/ prefix never reaches the matcher. Routing is done on
//     the subscriber's own MQTT patterns, which never carry it — it is added
//     only when the subscription is sent to the broker, and the broker delivers
//     the concrete topic.
//   - Routing and dispatch agree by construction. An entry matches exactly when
//     MQTTSubscriber.findSubscription will find a subscription, because both ask
//     topicmatch.Matches over the same patterns. Anything routed can be handled.
//
// That last point is why topicmatch is used rather than paho's own matcher: the
// two differ on $-prefixed topics, where a leading wildcard matches in paho and
// does not in topicmatch. Under the old arrangement a $SYS message could be
// routed to a subscriber whose only pattern was "#" and then rejected by
// findSubscription as unmatched, which is an error log for a message the router
// was sure about.
type subscriberRouter struct {
	mu      sync.RWMutex
	entries []routerEntry

	// aliasMu guards aliases alone, so registering one does not contend with
	// the entry list. MQTT 5 lets a broker send a topic once and refer to it by
	// alias afterwards; forgetting the mapping would strand every later message.
	aliasMu sync.Mutex
	aliases map[uint16]string

	// debug is guarded by mu with the entries, because SetDebugLogger is part
	// of the public interface and may be called at any time. paho does not call
	// it — paho.Client.SetDebugLogger does not forward to the router, and
	// autopaho only calls it when PahoDebug is set, which this client never
	// sets — so every line written through it is a no-op today and the race is
	// reachable by hand only. StandardRouter leaves the same field
	// unsynchronized; a mutex is cheaper than explaining why that is fine.
	debug log.Logger
}

// routerEntry is one recipient and every pattern that should reach it. The
// patterns are a set to test, not routes to iterate: matching several of them
// is still one delivery.
type routerEntry struct {
	patterns []string
	handler  paho.MessageHandler
}

func (e routerEntry) matches(topic string) bool {
	for _, p := range e.patterns {
		if topicmatch.Matches(p, topic) {
			return true
		}
	}
	return false
}

func newSubscriberRouter() *subscriberRouter {
	return &subscriberRouter{
		aliases: make(map[uint16]string),
		debug:   log.NOOPLogger{},
	}
}

// AddRecipient registers one handler behind any number of patterns, to be
// called at most once per inbound message however many of them match.
//
// The entry list is copy-on-write: Route takes a snapshot of the slice header
// and then dispatches with the lock released, so a mutator must never write
// into an array a snapshot may still be reading. The three-index append forces
// a fresh array rather than filling spare capacity in the old one.
//
// Dispatching outside the lock is the reason for the care. StandardRouter holds
// its read lock across every handler call, which is safe and also means a
// handler that touches the router deadlocks. Ours cannot, and pays for it here.
func (r *subscriberRouter) AddRecipient(patterns []string, handler paho.MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	frozen := r.entries[:len(r.entries):len(r.entries)]
	r.entries = append(frozen, routerEntry{patterns: patterns, handler: handler})
}

// RegisterHandler implements paho.Router. It is not how this router is
// populated — AddRecipient is — but it is the method an outside caller reaches
// for, so what it does and does not promise is worth being exact about.
//
// One pattern and one handler is *dispatched* the same way StandardRouter
// dispatches it: matched once, called once. Matching itself still follows
// topicmatch rather than paho's matcher, and the two differ twice. The $-topic
// rule above is one. The other is that StandardRouter strips a $share/<group>/
// prefix from the route it was registered under and this does not, so
// RegisterHandler("$share/g/a/#", h) would never fire — register the concrete
// filter instead. Recipients added through AddRecipient are unaffected, since
// a subscriber's own patterns never carry the prefix.
func (r *subscriberRouter) RegisterHandler(topic string, handler paho.MessageHandler) {
	r.logger().Println("registering handler for:", topic)
	r.AddRecipient([]string{topic}, handler)
}

// UnregisterHandler implements paho.Router, removing every entry registered
// under exactly this pattern.
//
// Into a new slice rather than compacting in place, for the reason on
// AddRecipient: an in-place compaction writes into the array a concurrent Route
// is iterating from its snapshot.
func (r *subscriberRouter) UnregisterHandler(topic string) {
	r.logger().Println("unregistering handler for:", topic)
	r.mu.Lock()
	defer r.mu.Unlock()

	kept := make([]routerEntry, 0, len(r.entries))
	for _, e := range r.entries {
		if len(e.patterns) == 1 && e.patterns[0] == topic {
			continue
		}
		kept = append(kept, e)
	}
	r.entries = kept
}

// Route implements paho.Router, delivering one inbound message to each
// recipient whose patterns match it — once each.
func (r *subscriberRouter) Route(pb *packets.Publish) {
	// paho.PublishFromPacketPublish dereferences Properties unconditionally, so
	// a packet without them panics there — StandardRouter included. Nothing
	// decoded off the wire arrives that way, since MQTT 5 always carries a
	// properties field even when it is empty, so this is not a live bug. It is
	// three lines to make the function total, and a router is a bad place to
	// take a process down from. The copy is shallow and local: the caller's
	// packet is not modified.
	if pb.Properties == nil {
		normalized := *pb
		normalized.Properties = &packets.Properties{}
		pb = &normalized
	}

	// One RLock for both, so a concurrent SetDebugLogger cannot land between
	// reading the entries and reading the logger.
	r.mu.RLock()
	entries, debug := r.entries, r.debug
	r.mu.RUnlock()

	m := paho.PublishFromPacketPublish(pb)
	m.Topic = r.resolveTopic(pb, debug)
	if m.Topic == "" {
		// Either an empty topic with no alias, or an alias nobody registered.
		// There is nothing to match against, and routing it to everyone would
		// be worse than dropping it. paho's matcher answers true for "#"
		// against an empty topic, so this declines where StandardRouter would
		// have delivered.
		debug.Println("dropping publish with no topic and no known alias")
		return
	}
	debug.Println("routing message for:", m.Topic)

	for _, e := range entries {
		if e.matches(m.Topic) {
			e.handler(m)
		}
	}
}

// resolveTopic answers what topic this packet is really on, registering a new
// alias or looking up an established one.
//
// The resolved topic is written back onto the Publish handed to the handler,
// which StandardRouter does not do: it resolves the alias to decide *routing*
// and then passes on a message whose Topic is still empty. A recipient that
// looks at the topic — as ours must, to pick a subscription and derive a
// vinculum topic — sees nothing and rejects the message.
//
// This is defence rather than a repair. Inbound aliasing is the server's to
// start and only if the client invited it: nothing here sets a connect-time
// Topic Alias Maximum, so it defaults to 0 and MQTT 5 §3.1.2.11.3 forbids a
// conforming server from sending an alias at all. Only a non-conforming broker
// reaches this path — and against one, delivery would stop after the first
// message on each topic, which is silent enough to be worth the dozen lines.
// Advertising a TopicAliasMaximum, if the bandwidth is ever wanted, is separate
// and larger work.
// Route owns the nil-Properties invariant and has already normalized, so this
// tests only for the alias.
func (r *subscriberRouter) resolveTopic(pb *packets.Publish, debug log.Logger) string {
	if pb.Properties.TopicAlias == nil {
		return pb.Topic
	}

	alias := *pb.Properties.TopicAlias

	r.aliasMu.Lock()
	defer r.aliasMu.Unlock()

	if pb.Topic != "" {
		debug.Printf("registering topic alias '%d' for topic '%s'", alias, pb.Topic)
		r.aliases[alias] = pb.Topic
		return pb.Topic
	}
	return r.aliases[alias]
}

// resetAliases forgets every alias mapping. Aliases are scoped to one
// connection, and this router outlives the connections it serves — it is built
// once in Start and reused across every reconnect — so a mapping from a dead
// session would otherwise answer for a number the new one has not assigned.
func (r *subscriberRouter) resetAliases() {
	r.aliasMu.Lock()
	defer r.aliasMu.Unlock()
	clear(r.aliases)
}

// SetDebugLogger implements paho.Router.
func (r *subscriberRouter) SetDebugLogger(l log.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.debug = l
}

// logger reads the debug logger for one use. Callers that also need the entry
// list take both under a single RLock instead.
func (r *subscriberRouter) logger() log.Logger {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.debug
}

// Ensure interface compliance.
var _ paho.Router = (*subscriberRouter)(nil)
