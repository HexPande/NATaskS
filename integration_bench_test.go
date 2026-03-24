package natasks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func BenchmarkIntegrationDispatch(b *testing.B) {
	env := newBenchmarkIntegrationEnv(b)
	client := env.newClientForBenchmark(b)
	task := benchmarkIntegrationTask(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := client.Dispatch(ctx, task, "emails"); err != nil {
			b.Fatalf("Dispatch() error = %v", err)
		}
	}
}

func BenchmarkIntegrationDispatchParallel(b *testing.B) {
	env := newBenchmarkIntegrationEnv(b)
	client := env.newClientForBenchmark(b)
	task := benchmarkIntegrationTask(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Dispatch(ctx, task, "emails"); err != nil {
				b.Fatalf("Dispatch() error = %v", err)
			}
		}
	})
}

func BenchmarkIntegrationEndToEnd(b *testing.B) {
	for _, tc := range []struct {
		name        string
		concurrency int
		fetchBatch  int
	}{
		{name: "serial", concurrency: 1, fetchBatch: 1},
		{name: "parallel_8", concurrency: 8, fetchBatch: 64},
	} {
		b.Run(tc.name, func(b *testing.B) {
			env := newBenchmarkIntegrationEnv(b)
			client := env.newClientForBenchmark(b)
			worker := env.newWorkerForBenchmark(b, "emails", tc.concurrency, tc.fetchBatch)
			task := benchmarkIntegrationTask(b)

			var processed atomic.Int64
			done := make(chan struct{}, 1)
			worker.Handle("emails.send", func(ctx context.Context, task *Task) error {
				if processed.Add(1) == int64(b.N) {
					select {
					case done <- struct{}{}:
					default:
					}
				}
				return nil
			})

			stopWorker := runWorkerForBenchmark(b, worker)
			defer stopWorker()

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := client.Dispatch(ctx, task, "emails"); err != nil {
					b.Fatalf("Dispatch() error = %v", err)
				}
			}

			select {
			case <-done:
			case <-time.After(30 * time.Second):
				b.Fatalf("timeout waiting for %d processed messages, got %d", b.N, processed.Load())
			}
			b.StopTimer()
		})
	}
}

func newBenchmarkIntegrationEnv(b *testing.B) integrationEnv {
	b.Helper()

	nc := mustNATSConnBenchmark(b)
	b.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatalf("jetstream.New() error = %v", err)
	}

	env := integrationEnv{
		nc:            nc,
		js:            js,
		streamName:    benchmarkToken(b, "stream"),
		subjectPrefix: benchmarkToken(b, "subject"),
	}

	b.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), env.streamName)
	})

	return env
}

func (e integrationEnv) newClientForBenchmark(b *testing.B) *Client {
	b.Helper()

	client, err := NewClient(
		e.js,
		WithStreamName(e.streamName),
		WithSubjectPrefix(e.subjectPrefix),
	)
	if err != nil {
		b.Fatalf("NewClient() error = %v", err)
	}

	return client
}

func (e integrationEnv) newWorkerForBenchmark(b *testing.B, queue string, concurrency, fetchBatch int) *Worker {
	b.Helper()

	worker, err := NewWorker(
		e.js,
		queue,
		WithStreamName(e.streamName),
		WithSubjectPrefix(e.subjectPrefix),
		WithConsumerPrefix(benchmarkToken(b, "consumer")),
		WithConcurrency(concurrency),
		WithFetchBatch(fetchBatch),
		WithFetchTimeout(500*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
	)
	if err != nil {
		b.Fatalf("NewWorker() error = %v", err)
	}

	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return worker
}

func runWorkerForBenchmark(b *testing.B, worker *Worker) func() {
	b.Helper()

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerErr := make(chan error, 1)

	go func() {
		workerErr <- worker.Run(workerCtx)
	}()

	return func() {
		cancelWorker()
		select {
		case err := <-workerErr:
			if err != nil {
				b.Fatalf("worker.Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			b.Fatal("timeout waiting for worker shutdown")
		}
	}
}

func mustNATSConnBenchmark(b *testing.B) *nats.Conn {
	b.Helper()

	url := os.Getenv(testNATSURLKey)
	if url == "" {
		b.Skipf("%s is not set", testNATSURLKey)
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		b.Fatalf("nats.Connect() error = %v", err)
	}

	return nc
}

func benchmarkToken(b *testing.B, prefix string) string {
	b.Helper()

	name := strings.ReplaceAll(b.Name(), "/", "-")
	return sanitizeToken(fmt.Sprintf("%s-%s-%d", prefix, name, time.Now().UnixNano()))
}

func benchmarkIntegrationTask(b *testing.B) *Task {
	b.Helper()

	task, err := NewTask("emails.send", []byte(`{"user_id":42,"email":"user@example.com","tags":["a","b","c"]}`))
	if err != nil {
		b.Fatalf("NewTask() error = %v", err)
	}

	return task
}
