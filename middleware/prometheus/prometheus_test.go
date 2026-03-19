package prometheus

import (
	"context"
	"errors"
	"testing"

	"github.com/hexpande/natasks"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDispatchMiddleware(t *testing.T) {
	registry := prom.NewPedanticRegistry()
	metrics, err := New(Options{Registerer: registry, Namespace: "natasks", Subsystem: "test"})
	require.NoError(t, err)

	task, err := natasks.NewTask("emails.send", []byte(`{}`))
	require.NoError(t, err)

	var called bool
	dispatch := metrics.DispatchMiddleware()(func(ctx context.Context, task *natasks.Task, queue string) error {
		called = true
		return nil
	})
	require.NoError(t, dispatch(context.Background(), task, "emails"))
	require.True(t, called)
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.dispatchTotal.WithLabelValues("emails", "emails.send", metricStatusSuccess)))
}

func TestProcessMiddleware(t *testing.T) {
	registry := prom.NewPedanticRegistry()
	metrics, err := New(Options{Registerer: registry, Namespace: "natasks", Subsystem: "test"})
	require.NoError(t, err)

	task, err := natasks.NewTask("emails.send", []byte(`{}`))
	require.NoError(t, err)

	handler := metrics.ProcessMiddleware("emails")(func(ctx context.Context, task *natasks.Task) error {
		require.Equal(t, 1.0, testutil.ToFloat64(metrics.processInflight.WithLabelValues("emails", "emails.send")))
		return errors.New("boom")
	})
	err = handler(context.Background(), task)
	require.EqualError(t, err, "boom")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.processTotal.WithLabelValues("emails", "emails.send", metricStatusError)))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.processInflight.WithLabelValues("emails", "emails.send")))
}
