package client

// wsConn is a net.Conn + sync.Locker wrapper around a gorilla WebSocket
// connection that guarantees each MQTT packet is sent as a single WebSocket
// message frame.
//
// Background: paho's packets.WriteTo assembles an MQTT packet into a
// net.Buffers slice and calls net.Buffers.WriteTo(conn). For connections that
// don't implement the internal buffersWriter interface, net.Buffers.WriteTo
// calls conn.Write once per buffer, producing one WebSocket message per buffer.
// The MQTT-over-WebSocket spec (RFC 6455 §10 + MQTT §6) requires each MQTT
// control packet to be exactly one WebSocket message, so splitting a single
// MQTT packet across multiple frames causes the broker to reject it.
//
// packets.WriteTo detects sync.Locker on the connection and calls Lock/Unlock
// around all Write calls for a single packet. We exploit this: buffer all
// writes while locked and flush the accumulated bytes as one WebSocket message
// when Unlock is called.

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/gorilla/websocket"
)

type wsConn struct {
	ws  *websocket.Conn
	rio sync.Mutex  // guards concurrent reads
	wmu sync.Mutex  // guards concurrent writes / flush
	buf bytes.Buffer // accumulates writes while locked
	locked bool
	r   io.Reader  // current WebSocket message reader
}

// newWSConn dials a WebSocket MQTT connection and returns a wsConn.
func newWSConn(ctx context.Context, tlsCfg *tls.Config, u *url.URL) (net.Conn, error) {
	d := *websocket.DefaultDialer
	d.TLSClientConfig = tlsCfg
	d.Subprotocols = []string{"mqtt"}
	d.HandshakeTimeout = 10 * time.Second

	header := http.Header{}
	ws, _, err := d.DialContext(ctx, u.String(), header)
	if err != nil {
		return nil, fmt.Errorf("websocket dial %s: %w", u, err)
	}
	return &wsConn{ws: ws}, nil
}

// Lock is called by packets.WriteTo before writing an MQTT packet.
func (c *wsConn) Lock() {
	c.wmu.Lock()
	c.buf.Reset()
	c.locked = true
}

// Unlock flushes the accumulated packet bytes as a single WebSocket binary
// message, then releases the write mutex.
func (c *wsConn) Unlock() {
	c.locked = false
	if c.buf.Len() > 0 {
		_ = c.ws.WriteMessage(websocket.BinaryMessage, c.buf.Bytes())
		c.buf.Reset()
	}
	c.wmu.Unlock()
}

// Write accumulates bytes into the buffer while locked (normal path); falls
// back to a direct WebSocket message for unlocked writes (shouldn't happen
// during normal MQTT operation, but handled for safety).
func (c *wsConn) Write(p []byte) (int, error) {
	if c.locked {
		return c.buf.Write(p)
	}
	// Unlocked direct write — shouldn't happen in normal MQTT usage.
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Read reads from the WebSocket connection, advancing to the next message
// when the current one is exhausted.
func (c *wsConn) Read(p []byte) (int, error) {
	c.rio.Lock()
	defer c.rio.Unlock()
	for {
		if c.r == nil {
			var err error
			_, c.r, err = c.ws.NextReader()
			if err != nil {
				return 0, err
			}
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsConn) Close() error                       { return c.ws.Close() }
func (c *wsConn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error      { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

// wsAttemptConnection is used as autopaho.ClientConfig.AttemptConnection for
// ws:// and wss:// URLs so that our buffered wsConn is used instead of the
// default autopaho websocketConnector.
func wsAttemptConnection(ctx context.Context, tlsCfg *tls.Config, u *url.URL) (net.Conn, error) {
	return newWSConn(ctx, tlsCfg, u)
}

// makeAttemptConnection returns an AttemptConnection function that uses our
// buffered wsConn for WebSocket URLs and standard dialers for TCP/TLS.
func makeAttemptConnection(tlsCfg *tls.Config) func(context.Context, autopaho.ClientConfig, *url.URL) (net.Conn, error) {
	return func(ctx context.Context, cfg autopaho.ClientConfig, u *url.URL) (net.Conn, error) {
		switch strings.ToLower(u.Scheme) {
		case "ws", "wss":
			return wsAttemptConnection(ctx, tlsCfg, u)
		case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
			if cfg.TlsCfg != nil {
				d := tls.Dialer{Config: cfg.TlsCfg}
				return d.DialContext(ctx, "tcp", u.Host)
			}
			return tls.Dial("tcp", u.Host, tlsCfg)
		default: // mqtt, tcp, ""
			var d net.Dialer
			return d.DialContext(ctx, "tcp", u.Host)
		}
	}
}
