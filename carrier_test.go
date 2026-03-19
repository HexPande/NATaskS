package natasks

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestNATSHeaderCarrier(t *testing.T) {
	headers := nats.Header{}
	carrier := natsHeaderCarrier(headers)

	carrier.Set("X-Test", "value")

	require.Equal(t, "value", carrier.Get("X-Test"))
	require.Contains(t, carrier.Keys(), "X-Test")
}
