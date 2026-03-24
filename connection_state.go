package natasks

import "github.com/nats-io/nats.go"

// IsReady reports whether the underlying NATS connection is ready to accept new work.
// Ready means the connection is currently in CONNECTED state.
func (c *Client) IsReady() bool {
	return connectionStatus(c.jetStreamConn()) == nats.CONNECTED
}

// IsReady reports whether the underlying NATS connection is ready to accept new work.
// Ready means the connection is currently in CONNECTED state.
func (w *Worker) IsReady() bool {
	return connectionStatus(w.jetStreamConn()) == nats.CONNECTED
}

func (c *Client) jetStreamConn() *nats.Conn {
	if c == nil || c.js == nil {
		return nil
	}

	return c.js.Conn()
}

func (w *Worker) jetStreamConn() *nats.Conn {
	if w == nil || w.js == nil {
		return nil
	}

	return w.js.Conn()
}

func connectionStatus(nc *nats.Conn) nats.Status {
	if nc == nil {
		return nats.CLOSED
	}

	return nc.Status()
}

func connectionLastError(nc *nats.Conn) error {
	if nc == nil {
		return nil
	}

	return nc.LastError()
}
