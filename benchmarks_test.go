package natasks

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type benchmarkPayload struct {
	UserID int      `json:"user_id"`
	Email  string   `json:"email"`
	Tags   []string `json:"tags"`
}

func benchmarkTask(b *testing.B) *Task {
	b.Helper()

	task, err := NewTask("emails.send", []byte(`{"user_id":42,"email":"user@example.com","tags":["a","b","c"]}`))
	if err != nil {
		b.Fatalf("NewTask() error = %v", err)
	}

	task.WithMessageID("job-42")
	return task
}

func benchmarkWorker(handler Handler) *Worker {
	return &Worker{
		cfg: workerConfig{
			config: config{
				subjectPrefix: defaultSubjectPrefix,
			},
			consumerPrefix:   defaultConsumerPrefix,
			concurrency:      1,
			fetchBatch:       1,
			progressInterval: time.Hour,
			maxRetries:       -1,
		},
		queue: "emails",
		handlers: map[string]Handler{
			"emails.send": handler,
		},
	}
}

func benchmarkMsg() jetstream.Msg {
	return &testMsg{
		headers: nats.Header{
			headerTaskName:        []string{"emails.send"},
			jetstream.MsgIDHeader: []string{"job-42"},
		},
		data: []byte(`{"user_id":42,"email":"user@example.com","tags":["a","b","c"]}`),
		meta: &jetstream.MsgMetadata{NumDelivered: 1},
	}
}

func BenchmarkNormalizeQueue(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = normalizeQueue("  emails  ")
	}
}

func BenchmarkQueueSubject(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = queueSubject("app.tasks", "emails")
	}
}

func BenchmarkAttemptsHeaderValue(b *testing.B) {
	b.ReportAllocs()
	for i := range b.N {
		_ = attemptsHeaderValue(uint64(i))
	}
}

func BenchmarkNewTask(b *testing.B) {
	payload := []byte(`{"user_id":42,"email":"user@example.com","tags":["a","b","c"]}`)
	b.ReportAllocs()
	for b.Loop() {
		task, err := NewTask("emails.send", payload)
		if err != nil {
			b.Fatalf("NewTask() error = %v", err)
		}
		_ = task
	}
}

func BenchmarkTaskPayload(b *testing.B) {
	task := benchmarkTask(b)
	b.ReportAllocs()
	for b.Loop() {
		_ = task.Payload()
	}
}

func BenchmarkTaskUnmarshal(b *testing.B) {
	task := benchmarkTask(b)
	var payload benchmarkPayload

	b.ReportAllocs()
	for b.Loop() {
		payload = benchmarkPayload{}
		if err := task.Unmarshal(&payload); err != nil {
			b.Fatalf("Unmarshal() error = %v", err)
		}
	}
}

func BenchmarkNoRetry(b *testing.B) {
	err := errors.New("invalid payload")
	b.ReportAllocs()
	for b.Loop() {
		wrapped := NoRetry(err)
		if !errors.Is(wrapped, ErrNoRetry) {
			b.Fatal("wrapped error does not match ErrNoRetry")
		}
		_ = unwrapNoRetry(wrapped)
	}
}

func BenchmarkCollectWorkerConfig(b *testing.B) {
	opts := []WorkerOption{
		WithStreamName("APP"),
		WithSubjectPrefix("app.tasks"),
		WithConsumerPrefix("worker"),
		WithDurable("jobs"),
		WithConcurrency(8),
		WithFetchBatch(32),
		WithFetchTimeout(2 * time.Second),
		WithTaskTimeout(3 * time.Second),
		WithIdleWait(20 * time.Millisecond),
		WithAckWait(4 * time.Second),
		WithProgressInterval(time.Second),
		WithMaxAckPending(128),
		WithMaxRetries(5),
		WithRetryBackoff(time.Second, 2*time.Second, 3*time.Second),
		WithDLQSuffix(".dlq"),
	}

	b.ReportAllocs()
	for b.Loop() {
		cfg, err := collectWorkerConfig(opts)
		if err != nil {
			b.Fatalf("collectWorkerConfig() error = %v", err)
		}
		_ = cfg
	}
}

func BenchmarkChainDispatchMiddleware(b *testing.B) {
	var calls atomic.Int64
	final := func(ctx context.Context, task *Task, queue string) error {
		calls.Add(1)
		return nil
	}
	middlewares := []DispatchMiddleware{
		func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				return next(ctx, task, queue)
			}
		},
		func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				return next(ctx, task, queue)
			}
		},
		func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				return next(ctx, task, queue)
			}
		},
	}
	dispatch := chainDispatchMiddleware(final, middlewares)
	task := benchmarkTask(b)

	b.ReportAllocs()
	for b.Loop() {
		if err := dispatch(context.Background(), task, "emails"); err != nil {
			b.Fatalf("dispatch() error = %v", err)
		}
	}
}

func BenchmarkChainProcessMiddleware(b *testing.B) {
	var calls atomic.Int64
	final := func(ctx context.Context, task *Task) error {
		calls.Add(1)
		return nil
	}
	middlewares := []ProcessMiddleware{
		func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				return next(ctx, task)
			}
		},
		func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				return next(ctx, task)
			}
		},
		func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				return next(ctx, task)
			}
		},
	}
	handler := chainProcessMiddleware(final, middlewares)
	task := benchmarkTask(b)

	b.ReportAllocs()
	for b.Loop() {
		if err := handler(context.Background(), task); err != nil {
			b.Fatalf("handler() error = %v", err)
		}
	}
}

func BenchmarkClientNewMessage(b *testing.B) {
	client := &Client{cfg: config{subjectPrefix: "app.tasks"}}
	task := benchmarkTask(b)

	b.ReportAllocs()
	for b.Loop() {
		_ = client.newMessage(context.Background(), "emails", task)
	}
}

func BenchmarkClientNewScheduledMessage(b *testing.B) {
	client := &Client{cfg: config{subjectPrefix: "app.tasks"}}
	task := benchmarkTask(b)
	ctx := withDispatchScheduleAt(context.Background(), time.Date(2026, 3, 18, 10, 0, 0, 0, time.UTC))

	b.ReportAllocs()
	for b.Loop() {
		_ = client.newMessage(ctx, "emails", task)
	}
}

func BenchmarkClientDispatchTask(b *testing.B) {
	client := &Client{
		dispatch: func(ctx context.Context, task *Task, queue string) error {
			return nil
		},
	}
	task := benchmarkTask(b)

	b.ReportAllocs()
	for b.Loop() {
		if err := client.Dispatch(context.Background(), task, "emails"); err != nil {
			b.Fatalf("Dispatch() error = %v", err)
		}
	}
}

func BenchmarkWorkerMessageTask(b *testing.B) {
	w := benchmarkWorker(func(context.Context, *Task) error { return nil })
	msg := benchmarkMsg()

	b.ReportAllocs()
	for b.Loop() {
		task, handler, err := w.messageTask(msg)
		if err != nil {
			b.Fatalf("messageTask() error = %v", err)
		}
		if task == nil || handler == nil {
			b.Fatal("messageTask() returned nil task or handler")
		}
	}
}

func BenchmarkWorkerHandleMessageSuccess(b *testing.B) {
	w := benchmarkWorker(func(context.Context, *Task) error { return nil })

	b.ReportAllocs()
	for b.Loop() {
		if err := w.handleMessage(context.Background(), benchmarkMsg()); err != nil {
			b.Fatalf("handleMessage() error = %v", err)
		}
	}
}

func BenchmarkWorkerHandleMessageRetry(b *testing.B) {
	w := benchmarkWorker(func(context.Context, *Task) error { return errors.New("retry me") })
	w.cfg.retryBackoff = []time.Duration{time.Second}

	b.ReportAllocs()
	for b.Loop() {
		if err := w.handleMessage(context.Background(), benchmarkMsg()); err == nil {
			b.Fatal("handleMessage() error = nil")
		}
	}
}

func BenchmarkWorkerHandleMessageNoRetry(b *testing.B) {
	w := benchmarkWorker(func(context.Context, *Task) error { return NoRetry(errors.New("do not retry")) })

	b.ReportAllocs()
	for b.Loop() {
		if err := w.handleMessage(context.Background(), benchmarkMsg()); err == nil {
			b.Fatal("handleMessage() error = nil")
		}
	}
}

func BenchmarkWorkerProcessBatch(b *testing.B) {
	for _, tc := range []struct {
		name        string
		concurrency int
		size        int
	}{
		{name: "serial", concurrency: 1, size: 32},
		{name: "parallel_8", concurrency: 8, size: 32},
	} {
		b.Run(tc.name, func(b *testing.B) {
			w := benchmarkWorker(func(context.Context, *Task) error { return nil })
			w.cfg.concurrency = tc.concurrency
			w.cfg.fetchBatch = tc.size

			msgs := make([]jetstream.Msg, 0, tc.size)
			for range tc.size {
				msgs = append(msgs, benchmarkMsg())
			}

			b.ReportAllocs()
			for b.Loop() {
				w.processBatch(context.Background(), msgs)
			}
		})
	}
}

func BenchmarkWorkerRetryHelpers(b *testing.B) {
	w := &Worker{cfg: workerConfig{fetchBatch: 8, concurrency: 16, maxRetries: 5, retryBackoff: []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}}}

	b.Run("exhausted_retries", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = w.exhaustedRetries(uint64(i))
		}
	})

	b.Run("redelivery_delay", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			_ = w.redeliveryDelay(uint64(i))
		}
	})

	b.Run("fetch_size", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = w.fetchSize()
		}
	})

	b.Run("consumer_name", func(b *testing.B) {
		w.cfg.consumerPrefix = "worker"
		w.queue = "emails.send"
		b.ReportAllocs()
		for b.Loop() {
			_ = w.consumerName()
		}
	})
}
