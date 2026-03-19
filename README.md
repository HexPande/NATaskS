# natasks

`natasks` is a small Go task queue library built on top of NATS JetStream.

It focuses on two things:

- dispatching tasks
- processing tasks with workers

## Features

- JetStream-based task dispatching
- delayed task dispatch
- publish deduplication via `Nats-Msg-Id`
- worker-based task processing
- automatic stream and consumer provisioning
- retry policy with backoff and DLQ
- graceful shutdown
- lease renewal for long-running handlers via `InProgress`
- dispatch and processing middleware
- OpenTelemetry propagation support
- Prometheus middleware

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

func main() {
	nc, err := nats.Connect("nats://127.0.0.1:4222")
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
		natasks.WithMaxRetries(3),
		natasks.WithRetryBackoff(500*time.Millisecond, time.Second, 2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("emails.send", func(ctx context.Context, task *natasks.Task) error {
		var payload struct {
			UserID int    `json:"user_id"`
			Email  string `json:"email"`
		}

		if err := task.Unmarshal(&payload); err != nil {
			return err
		}

		log.Printf("send email to %s for user %d", payload.Email, payload.UserID)
		return nil
	})

	body, err := json.Marshal(map[string]any{
		"user_id": 42,
		"email":   "user@example.com",
	})
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

## API

Main constructors:

- `NewClient(js jetstream.JetStream, opts ...Option)`
- `NewWorker(js jetstream.JetStream, queue string, opts ...WorkerOption)`

Main methods:

- `client.Dispatch(ctx, task, queue)`
- `client.DispatchIn(ctx, task, queue, delay)`
- `client.DispatchAt(ctx, task, queue, at)`
- `worker.Handle(name, handler)`
- `worker.Run(ctx)`
- `task.WithMessageID(id)`

## Retry and DLQ

If a handler returns an error, the worker can retry the task and eventually move it to a dead-letter queue.

Worker options:

- `WithMaxRetries(n)`
- `WithRetryBackoff(delays...)`
- `WithDLQSuffix(suffix)`

Defaults:

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

Observability packages:

- `github.com/hexpande/natasks/middleware/otel`
- `github.com/hexpande/natasks/middleware/prometheus`

## Configuration

Shared options:

- `WithStreamName(name)`
- `WithSubjectPrefix(prefix)`
- `WithDispatchMiddleware(middleware...)`
- `WithPropagator(propagator)`

Worker options:

- `WithConsumerPrefix(prefix)`
- `WithDurable(name)`
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
