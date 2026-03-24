# NATaskS

![Logo](.github/logo.png)

[![Test](https://github.com/HexPande/NATaskS/actions/workflows/test.yml/badge.svg)](https://github.com/HexPande/NATaskS/actions/workflows/test.yml)
[![Lint](https://github.com/HexPande/NATaskS/actions/workflows/lint.yml/badge.svg)](https://github.com/HexPande/NATaskS/actions/workflows/lint.yml)
![Go Version](https://img.shields.io/github/go-mod/go-version/HexPande/NATaskS)
[![Go Reference](https://pkg.go.dev/badge/github.com/HexPande/NATaskS.svg)](https://pkg.go.dev/github.com/HexPande/NATaskS)
[![Go Report Card](https://goreportcard.com/badge/github.com/HexPande/NATaskS)](https://goreportcard.com/report/github.com/HexPande/NATaskS)


`NATaskS` is a small Go task queue library built on top of NATS JetStream.

It focuses on two things:

- dispatching tasks
- processing tasks with workers

## Features

- task dispatch on top of NATS JetStream
- immediate, delayed, and scheduled task delivery
- publish deduplication via `Nats-Msg-Id`
- worker-based task processing with configurable concurrency
- automatic stream and consumer provisioning
- retries with backoff and dead-letter queues
- graceful worker shutdown
- lease renewal for long-running handlers via `InProgress`
- dispatch and processing middleware
- OpenTelemetry context propagation
- Prometheus metrics middleware

## Installation

```bash
go get github.com/hexpande/natasks
```

## Requirements

- Go `1.25.6+`
- NATS with JetStream enabled

## Quick Start

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SendEmailPayload struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
}

func main() {
	nc, err := nats.Connect(
		"nats://127.0.0.1:4222",
		nats.Name("natasks-example"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(250*time.Millisecond, 2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	client, err := natasks.NewClient(js)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := natasks.NewWorker(
		js,
		"emails",
		natasks.WithConcurrency(8),
		natasks.WithMaxRetries(3),
		natasks.WithRetryBackoff(500*time.Millisecond, time.Second, 2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("emails.send", func(ctx context.Context, task *natasks.Task) error {
		var payload SendEmailPayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		log.Printf("send email to %s for user %d", payload.Email, payload.UserID)
		return nil
	})

	payload := SendEmailPayload{
		UserID: 42,
		Email:  "user@example.com",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}

	task, err := natasks.NewTask("emails.send", body)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Dispatch(context.Background(), task, "emails"); err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

## Recommended `nats.Connect`

For production workers and dispatchers, prefer explicit reconnect settings and callbacks instead of bare `nats.Connect(url)`:

- `RetryOnFailedConnect(true)` so process startup can wait for NATS instead of failing immediately.
- `MaxReconnects(-1)` so a long-lived worker keeps trying until your process decides to exit.
- `ReconnectWait(...)` and `ReconnectJitter(...)` to avoid reconnect storms.
- `DisconnectErrHandler`, `ReconnectHandler`, `ClosedHandler`, and `ErrorHandler` for observability.

`natasks` uses the connection you provide. It does not create or own `nats.Conn`, so reconnect policy should be defined at the `nats.Connect(...)` layer.

## Examples

Runnable examples live in [`examples/`](./examples):

- [`examples/basic`](./examples/basic)
- [`examples/delayed`](./examples/delayed)
- [`examples/retries-dlq`](./examples/retries-dlq)
- [`examples/middleware-headers`](./examples/middleware-headers)
- [`examples/otel`](./examples/otel)
- [`examples/prometheus`](./examples/prometheus)

See [`examples/README.md`](./examples/README.md) for a short index and usage notes.

## API

Main constructors:

- `NewClient(js jetstream.JetStream, opts ...Option)`
- `NewWorker(js jetstream.JetStream, queue string, opts ...WorkerOption)`

Main methods:

- `client.Dispatch(ctx, task, queue)`
- `client.DispatchIn(ctx, task, queue, delay)`
- `client.DispatchAt(ctx, task, queue, at)`
- `client.IsReady()`
- `worker.Handle(name, handler)`
- `worker.Run(ctx)`
- `worker.IsReady()`
- `task.WithMessageID(id)`

## Connection Loss Behavior

`worker.Run(ctx)` treats temporary NATS disconnects as recoverable runtime events.

- While the underlying connection is reconnecting, the worker pauses fetches.
- After reconnect, it ensures the stream and consumer still exist and then resumes processing.
- `Run` returns only when `ctx` is canceled, the NATS connection is permanently closed, or a non-recoverable fetch error occurs.

Use `worker.IsReady()` or `client.IsReady()` when you need a simple readiness check.

## Retry and DLQ

If a handler returns an error, the worker can retry the task and eventually move it to a dead-letter queue.

If a handler should fail without retries and without DLQ publication, return `natasks.NoRetry(err)`.

```go
worker.Handle("emails.send", func(ctx context.Context, task *natasks.Task) error {
	if err := validate(task); err != nil {
		return natasks.NoRetry(err)
	}

	return sendEmail(ctx, task)
})
```

Worker options:

- `WithConcurrency(n)`
- `WithMaxRetries(n)`
- `WithRetryBackoff(delays...)`
- `WithDLQSuffix(suffix)`

Defaults:

- `concurrency`: `1`
- `max retries`: `-1` (unlimited)
- `retry backoff`: none
- `dlq suffix`: `-dlq`

DLQ messages keep the original payload and include these headers:

- `Natasks-Original-Queue`
- `Natasks-Attempts`
- `Natasks-Last-Error`

## Middleware

Core middleware types:

- `DispatchMiddleware`
- `ProcessMiddleware`

Task headers are available directly in middleware through `Task`:

```go
natasks.WithDispatchMiddleware(func(next natasks.DispatchFunc) natasks.DispatchFunc {
	return func(ctx context.Context, task *natasks.Task, queue string) error {
		task.SetHeader("X-Request-ID", "req-42")
		return next(ctx, task, queue)
	}
})

natasks.WithProcessMiddleware(func(next natasks.Handler) natasks.Handler {
	return func(ctx context.Context, task *natasks.Task) error {
		requestID := task.Header("X-Request-ID")
		_ = requestID
		return next(ctx, task)
	}
})
```

Use `WithPropagator(...)` when you want to map values between `context.Context` and headers automatically.

Observability packages:

- `github.com/hexpande/natasks/middleware/otel`
- `github.com/hexpande/natasks/middleware/prometheus`

OpenTelemetry requires two pieces:

- `otel.Middleware` for dispatch and process spans
- `natasks.WithPropagator(otelMiddleware)` for trace-context propagation through message headers

Example:

```go
otelMiddleware := otel.New(otel.Options{})

client, err := natasks.NewClient(
	js,
	natasks.WithPropagator(otelMiddleware),
	natasks.WithDispatchMiddleware(otelMiddleware.DispatchMiddleware()),
)
if err != nil {
	log.Fatal(err)
}

worker, err := natasks.NewWorker(
	js,
	"emails",
	natasks.WithPropagator(otelMiddleware),
	natasks.WithProcessMiddleware(otelMiddleware.ProcessMiddleware("emails")),
)
if err != nil {
	log.Fatal(err)
}
```

## Configuration

Shared options:

- `WithStreamName(name)`
- `WithSubjectPrefix(prefix)`
- `WithDispatchMiddleware(middleware...)`
- `WithPropagator(propagator)`

Worker options:

- `WithConsumerPrefix(prefix)`
- `WithDurable(name)`
- `WithConcurrency(n)`
- `WithFetchBatch(size)`
- `WithFetchTimeout(timeout)`
- `WithIdleWait(wait)`
- `WithTaskTimeout(timeout)`
- `WithAckWait(wait)`
- `WithProgressInterval(interval)`
- `WithMaxAckPending(n)`
- `WithMaxRetries(n)`
- `WithRetryBackoff(delays...)`
- `WithDLQSuffix(suffix)`
- `WithProcessMiddleware(middleware...)`

## Testing

```bash
go test ./...
make docker-test
```

Integration tests use a real NATS JetStream instance via Docker Compose.

## Benchmarking

Run in-process microbenchmarks:

```bash
go test -run '^$' -bench . -benchmem ./...
```

For more stable numbers, prefer a longer bench time and multiple runs:

```bash
go test -run '^$' -bench . -benchmem -benchtime=2s -count=5 ./...
```

Live NATS integration/perf benchmarks are available for real JetStream dispatch and end-to-end worker processing. They require a reachable NATS server and `NATASKS_NATS_URL`:

```bash
NATASKS_NATS_URL=nats://127.0.0.1:4222 go test -run '^$' -bench 'Integration' -benchmem ./...
```

For more reliable integration numbers, vary CPU and run multiple samples:

```bash
NATASKS_NATS_URL=nats://127.0.0.1:4222 go test -run '^$' -bench 'Integration' -benchmem -benchtime=2s -count=5 -cpu=1,8 ./...
```

Example live integration results on Apple M2 with local NATS:

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `IntegrationDispatch-8` | `48849` | `2217` | `30` |
| `IntegrationDispatchParallel-8` | `12352` | `2249` | `30` |
| `IntegrationEndToEnd/serial-8` | `136070` | `7107` | `94` |
| `IntegrationEndToEnd/parallel_8-8` | `101821` | `4982` | `60` |

These numbers are environment-specific, but they show the expected shape: parallel dispatch improves throughput, and a worker with `WithConcurrency(8)` outperforms serial end-to-end processing.

To compare changes between revisions, save results and use `benchstat`:

```bash
go test -run '^$' -bench . -benchmem -benchtime=3s -count=10 ./... > before.txt
go test -run '^$' -bench . -benchmem -benchtime=3s -count=10 ./... > after.txt
benchstat before.txt after.txt
```
