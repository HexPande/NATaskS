package otel

import (
	"context"
	"testing"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func benchmarkTask(b *testing.B) *natasks.Task {
	b.Helper()

	task, err := natasks.NewTask("emails.send", []byte(`{"user_id":42}`))
	if err != nil {
		b.Fatalf("NewTask() error = %v", err)
	}

	return task
}

func BenchmarkInject(b *testing.B) {
	provider := sdktrace.NewTracerProvider()
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})
	ctx, span := provider.Tracer("bench").Start(context.Background(), "root")
	defer span.End()

	b.ReportAllocs()
	for b.Loop() {
		header := nats.Header{}
		mw.Inject(ctx, natsHeaderCarrier(header))
	}
}

func BenchmarkExtract(b *testing.B) {
	provider := sdktrace.NewTracerProvider()
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})
	ctx, span := provider.Tracer("bench").Start(context.Background(), "root")
	defer span.End()

	header := nats.Header{}
	mw.Inject(ctx, natsHeaderCarrier(header))

	b.ReportAllocs()
	for b.Loop() {
		_ = mw.Extract(context.Background(), natsHeaderCarrier(header))
	}
}

func BenchmarkDispatchMiddleware(b *testing.B) {
	provider := sdktrace.NewTracerProvider()
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})
	task := benchmarkTask(b)
	dispatch := mw.DispatchMiddleware()(func(ctx context.Context, task *natasks.Task, queue string) error { return nil })

	b.ReportAllocs()
	for b.Loop() {
		if err := dispatch(context.Background(), task, "emails"); err != nil {
			b.Fatalf("dispatch() error = %v", err)
		}
	}
}

func BenchmarkProcessMiddleware(b *testing.B) {
	provider := sdktrace.NewTracerProvider()
	mw := New(Options{TracerProvider: provider, Propagator: propagation.TraceContext{}})
	task := benchmarkTask(b)
	handler := mw.ProcessMiddleware("emails")(func(ctx context.Context, task *natasks.Task) error { return nil })

	b.ReportAllocs()
	for b.Loop() {
		if err := handler(context.Background(), task); err != nil {
			b.Fatalf("handler() error = %v", err)
		}
	}
}
