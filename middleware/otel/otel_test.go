package otel

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDispatchMiddlewareCreatesProducerSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})

	task, err := natasks.NewTask("emails.send", []byte(`{}`))
	require.NoError(t, err)
	dispatch := mw.DispatchMiddleware()(func(ctx context.Context, task *natasks.Task, queue string) error { return nil })
	require.NoError(t, dispatch(context.Background(), task, "emails"))

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "natasks dispatch", spans[0].Name())
	require.Equal(t, codes.Unset, spans[0].Status().Code)
	require.Contains(t, spans[0].Attributes(), attribute.String("natasks.task", "emails.send"))
}

func TestPropagationFromDispatchToProcess(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})

	ctx, root := provider.Tracer("test").Start(context.Background(), "root")

	body, err := json.Marshal(map[string]string{"kind": "trace"})
	require.NoError(t, err)
	task, err := natasks.NewTask("jobs.trace", body)
	require.NoError(t, err)

	msg := nats.NewMsg("natasks.emails")
	msg.Header = nats.Header{}
	msg.Header.Set("Natasks-Task", task.Name())
	msg.Header.Set("Natasks-Queue", "emails")

	mw.Inject(ctx, natsHeaderCarrier(msg.Header))
	extracted := mw.Extract(context.Background(), natsHeaderCarrier(msg.Header))
	_, child := provider.Tracer("test").Start(extracted, "child")
	child.End()
	root.End()

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, spans[1].SpanContext().TraceID(), spans[0].Parent().TraceID())
}

type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string { return nats.Header(c).Get(key) }
func (c natsHeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}
