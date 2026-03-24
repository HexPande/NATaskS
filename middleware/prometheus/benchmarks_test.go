package prometheus

import (
	"context"
	"testing"

	"github.com/hexpande/natasks"
	prom "github.com/prometheus/client_golang/prometheus"
)

func benchmarkMetrics(b *testing.B) *Metrics {
	b.Helper()

	metrics, err := New(Options{
		Registerer: prom.NewRegistry(),
		Namespace:  "natasks",
		Subsystem:  "bench",
	})
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}

	return metrics
}

func benchmarkTask(b *testing.B) *natasks.Task {
	b.Helper()

	task, err := natasks.NewTask("emails.send", []byte(`{"user_id":42}`))
	if err != nil {
		b.Fatalf("NewTask() error = %v", err)
	}

	return task
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		metrics, err := New(Options{
			Registerer: prom.NewRegistry(),
			Namespace:  "natasks",
			Subsystem:  "bench",
		})
		if err != nil {
			b.Fatalf("New() error = %v", err)
		}
		_ = metrics
	}
}

func BenchmarkDispatchMiddleware(b *testing.B) {
	metrics := benchmarkMetrics(b)
	task := benchmarkTask(b)
	dispatch := metrics.DispatchMiddleware()(func(ctx context.Context, task *natasks.Task, queue string) error { return nil })

	b.ReportAllocs()
	for b.Loop() {
		if err := dispatch(context.Background(), task, "emails"); err != nil {
			b.Fatalf("dispatch() error = %v", err)
		}
	}
}

func BenchmarkProcessMiddleware(b *testing.B) {
	metrics := benchmarkMetrics(b)
	task := benchmarkTask(b)
	handler := metrics.ProcessMiddleware("emails")(func(ctx context.Context, task *natasks.Task) error { return nil })

	b.ReportAllocs()
	for b.Loop() {
		if err := handler(context.Background(), task); err != nil {
			b.Fatalf("handler() error = %v", err)
		}
	}
}
