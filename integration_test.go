package natasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testNATSURLKey = "NATASKS_NATS_URL"

type contextKey string

func TestIntegrationDispatchAndProcess(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "emails"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	type payload struct {
		UserID int    `json:"user_id"`
		Email  string `json:"email"`
	}

	received := make(chan payload, 1)
	worker.Handle("emails.send", func(ctx context.Context, task *Task) error {
		var got payload
		if err := task.Unmarshal(&got); err != nil {
			return err
		}

		select {
		case received <- got:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(payload{
		UserID: 42,
		Email:  "user@example.com",
	})
	require.NoError(t, err)

	task, err := NewTask("emails.send", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case got := <-received:
		require.Equal(t, payload{UserID: 42, Email: "user@example.com"}, got)
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for task processing")
	}
}

func TestIntegrationDispatchInDelaysDelivery(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "emails"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	deliveredAt := make(chan time.Time, 1)
	worker.Handle("emails.delayed", func(ctx context.Context, task *Task) error {
		select {
		case deliveredAt <- time.Now():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]any{"kind": "delayed"})
	require.NoError(t, err)

	task, err := NewTask("emails.delayed", body)
	require.NoError(t, err)

	startedAt := time.Now()
	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.DispatchIn(dispatchCtx, task, queue, 400*time.Millisecond))

	select {
	case gotAt := <-deliveredAt:
		require.GreaterOrEqual(t, gotAt.Sub(startedAt), 250*time.Millisecond)
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for delayed task")
	}
}

func TestIntegrationRetryOnHandlerError(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "reports"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	worker.Handle("reports.generate", func(ctx context.Context, task *Task) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient error")
		}

		select {
		case done <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	body, err := json.Marshal(map[string]string{"kind": "weekly"})
	require.NoError(t, err)

	task, err := NewTask("reports.generate", body)
	require.NoError(t, err)

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for retried task")
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestIntegrationTaskTimeoutRetriesHandler(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "timeouts"
	client := env.newClient(t)

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(200*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithTaskTimeout(100*time.Millisecond),
		WithMaxRetries(2),
		WithRetryBackoff(10*time.Millisecond),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	worker.Handle("jobs.timeout", func(ctx context.Context, task *Task) error {
		if attempts.Add(1) == 1 {
			<-ctx.Done()
			return ctx.Err()
		}

		select {
		case done <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "timeout"})
	require.NoError(t, err)

	task, err := NewTask("jobs.timeout", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for timed out task retry")
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestIntegrationPanicRecoveryRetriesHandler(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "panics"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	worker.Handle("jobs.panic", func(ctx context.Context, task *Task) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}

		select {
		case done <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "panic"})
	require.NoError(t, err)

	task, err := NewTask("jobs.panic", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for panic-recovered task retry")
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
}

func TestIntegrationRetryBackoffDelaysNextAttempt(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "backoff"
	client := env.newClient(t)

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(200*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithMaxRetries(2),
		WithRetryBackoff(300*time.Millisecond),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	firstAttemptAt := make(chan time.Time, 1)
	secondAttemptAt := make(chan time.Time, 1)
	var attempts atomic.Int32
	worker.Handle("jobs.backoff", func(ctx context.Context, task *Task) error {
		switch attempts.Add(1) {
		case 1:
			firstAttemptAt <- time.Now()
			return errors.New("retry me later")
		case 2:
			secondAttemptAt <- time.Now()
			return nil
		default:
			return errors.New("unexpected extra retry")
		}
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "backoff"})
	require.NoError(t, err)
	task, err := NewTask("jobs.backoff", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	var firstAt time.Time
	select {
	case firstAt = <-firstAttemptAt:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for first attempt")
	}

	select {
	case secondAt := <-secondAttemptAt:
		require.GreaterOrEqual(t, secondAt.Sub(firstAt), 250*time.Millisecond)
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for second attempt")
	}
}

func TestIntegrationRetryExhaustedPublishesToDLQ(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "reports"
	client := env.newClient(t)

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(200*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithMaxRetries(2),
		WithRetryBackoff(10*time.Millisecond, 10*time.Millisecond),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	var attempts atomic.Int32
	worker.Handle("reports.fail", func(ctx context.Context, task *Task) error {
		attempts.Add(1)
		return errors.New("permanent failure")
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	dlqName := dlqQueue(queue, defaultDLQSuffix)
	sub := env.inspectConsumer(t, queueSubject(env.subjectPrefix, dlqName))

	body, err := json.Marshal(map[string]string{"kind": "fail"})
	require.NoError(t, err)
	task, err := NewTask("reports.fail", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	var dlqMsg jetstream.Msg
	select {
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
	}

	dlqMsg = mustFetchOne(t, sub, 10*time.Second)
	require.Equal(t, dlqName, dlqMsg.Headers().Get(headerQueueName))
	require.Equal(t, queue, dlqMsg.Headers().Get(headerOriginalQueue))
	require.Equal(t, "reports.fail", dlqMsg.Headers().Get(headerTaskName))
	require.Equal(t, "3", dlqMsg.Headers().Get(headerAttempts))
	require.Equal(t, "permanent failure", dlqMsg.Headers().Get(headerLastError))
	require.NoError(t, dlqMsg.Ack())
	require.Equal(t, int32(3), attempts.Load())
}

func TestIntegrationNoRetryAcknowledgesWithoutRedelivery(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "noretry"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	worker.Handle("jobs.noretry", func(ctx context.Context, task *Task) error {
		attempts.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
		return NoRetry(errors.New("invalid payload"))
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	dlqName := dlqQueue(queue, defaultDLQSuffix)
	sub := env.inspectConsumer(t, queueSubject(env.subjectPrefix, dlqName))

	body, err := json.Marshal(map[string]string{"kind": "invalid"})
	require.NoError(t, err)
	task, err := NewTask("jobs.noretry", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for non-retriable task")
	}

	select {
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(300 * time.Millisecond):
	}

	require.Equal(t, int32(1), attempts.Load())

	dlqFetchCtx, cancelFetch := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFetch()
	msgs, err := sub.Fetch(1, jetstream.FetchContext(dlqFetchCtx))
	require.NoError(t, err)
	var gotDLQ bool
	for range msgs.Messages() {
		gotDLQ = true
	}
	require.NoError(t, msgs.Error())
	require.False(t, gotDLQ, "unexpected message in DLQ")
}

func TestIntegrationDispatchPublishesHeadersAndPayload(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "billing"
	client := env.newClient(t)

	type payload struct {
		InvoiceID string `json:"invoice_id"`
	}

	body, err := json.Marshal(payload{InvoiceID: "inv-42"})
	require.NoError(t, err)

	task, err := NewTask("billing.charge", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	sub := env.inspectConsumer(t, queueSubject(env.subjectPrefix, queue))

	msg := mustFetchOne(t, sub, 5*time.Second)
	require.Equal(t, "billing.charge", msg.Headers().Get(headerTaskName))
	require.Equal(t, queue, msg.Headers().Get(headerQueueName))

	gotTask, err := NewTask(msg.Headers().Get(headerTaskName), msg.Data())
	require.NoError(t, err)

	var got payload
	require.NoError(t, gotTask.Unmarshal(&got))
	require.Equal(t, payload{InvoiceID: "inv-42"}, got)

	require.NoError(t, msg.Ack())
}

func TestIntegrationDispatchDeduplicatesByMessageID(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	client := env.newClient(t)

	body, err := json.Marshal(map[string]string{"kind": "dedup"})
	require.NoError(t, err)

	firstTask, err := NewTask("jobs.dedup", body)
	require.NoError(t, err)
	firstTask.WithMessageID("job-42")

	secondTask, err := NewTask("jobs.dedup", body)
	require.NoError(t, err)
	secondTask.WithMessageID("job-42")

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	require.NoError(t, client.Dispatch(dispatchCtx, firstTask, "alpha"))
	require.NoError(t, client.Dispatch(dispatchCtx, secondTask, "alpha"))

	info := env.streamInfo(t)
	require.EqualValues(t, 1, info.State.Msgs)
}

func TestIntegrationClientCreatesExpectedStreamConfig(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	_ = env.newClient(t)

	info := env.streamInfo(t)
	require.Equal(t, jetstream.WorkQueuePolicy, info.Config.Retention)
	require.Equal(t, jetstream.FileStorage, info.Config.Storage)
	require.Len(t, info.Config.Subjects, 1)
	require.Equal(t, queueSubject(env.subjectPrefix, "*"), info.Config.Subjects[0])
	require.True(t, info.Config.AllowMsgSchedules)
}

func TestIntegrationWorkerCreatesExpectedConsumerConfig(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "emails"
	durable := testToken(t, "durable")

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDurable(durable),
		WithAckWait(3*time.Second),
		WithProgressInterval(1*time.Second),
		WithFetchTimeout(500*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithMaxAckPending(7),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	info := env.consumerInfo(t, worker.consumerName())
	require.Equal(t, durable, info.Config.Durable)
	require.Equal(t, queueSubject(env.subjectPrefix, queue), info.Config.FilterSubject)
	require.Equal(t, 3*time.Second, info.Config.AckWait)
	require.Equal(t, 7, info.Config.MaxAckPending)
	require.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)
}

func TestIntegrationDispatchMiddlewareCanRewriteQueue(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	client, err := NewClient(
		env.js,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDispatchMiddleware(func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				return next(ctx, task, "beta")
			}
		}),
	)
	require.NoError(t, err)

	body, err := json.Marshal(map[string]string{"kind": "rerouted"})
	require.NoError(t, err)
	task, err := NewTask("jobs.reroute", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, "alpha"))

	sub := env.inspectConsumer(t, queueSubject(env.subjectPrefix, "beta"))

	msg := mustFetchOne(t, sub, 5*time.Second)
	require.Equal(t, "beta", msg.Headers().Get(headerQueueName))
	require.Equal(t, "jobs.reroute", msg.Headers().Get(headerTaskName))
	require.NoError(t, msg.Ack())
}

func TestIntegrationDispatchMiddlewareCanShortCircuit(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	client, err := NewClient(
		env.js,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDispatchMiddleware(func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				return errors.New("blocked by middleware")
			}
		}),
	)
	require.NoError(t, err)

	body, err := json.Marshal(map[string]string{"kind": "blocked"})
	require.NoError(t, err)
	task, err := NewTask("jobs.blocked", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	err = client.Dispatch(dispatchCtx, task, "alpha")
	require.EqualError(t, err, "blocked by middleware")

	info := env.streamInfo(t)
	require.EqualValues(t, 0, info.State.Msgs)
}

func TestIntegrationNewWorkerReusesExistingConsumer(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "reused"
	durable := testToken(t, "durable")

	first, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDurable(durable),
	)
	require.NoError(t, err)

	second, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDurable(durable),
	)
	require.NoError(t, err)
	require.Equal(t, first.consumerName(), second.consumerName())
}

func TestIntegrationNewClientReusesExistingStream(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)

	first := env.newClient(t)
	second := env.newClient(t)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	firstBody, err := json.Marshal(map[string]int{"step": 1})
	require.NoError(t, err)
	firstTask, err := NewTask("jobs.first", firstBody)
	require.NoError(t, err)

	secondBody, err := json.Marshal(map[string]int{"step": 2})
	require.NoError(t, err)
	secondTask, err := NewTask("jobs.second", secondBody)
	require.NoError(t, err)

	require.NoError(t, first.Dispatch(dispatchCtx, firstTask, "alpha"))
	require.NoError(t, second.Dispatch(dispatchCtx, secondTask, "beta"))

	info := env.streamInfo(t)
	require.EqualValues(t, 2, info.State.Msgs)
}

func TestIntegrationQueueIsolation(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	client := env.newClient(t)
	alpha := env.newWorker(t, "alpha")
	beta := env.newWorker(t, "beta")

	alphaDone := make(chan struct{}, 1)
	betaDone := make(chan struct{}, 1)

	alpha.Handle("jobs.alpha", func(ctx context.Context, task *Task) error {
		select {
		case alphaDone <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	beta.Handle("jobs.beta", func(ctx context.Context, task *Task) error {
		select {
		case betaDone <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	alphaErr, stopAlpha := runWorker(t, alpha)
	defer stopAlpha()
	betaErr, stopBeta := runWorker(t, beta)
	defer stopBeta()

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	body, err := json.Marshal(map[string]string{"queue": "beta"})
	require.NoError(t, err)
	task, err := NewTask("jobs.beta", body)
	require.NoError(t, err)

	require.NoError(t, client.Dispatch(dispatchCtx, task, "beta"))

	select {
	case <-betaDone:
	case err := <-alphaErr:
		require.NoError(t, err)
	case err := <-betaErr:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for beta queue")
	}

	select {
	case <-alphaDone:
		require.FailNow(t, "alpha worker handled task from beta queue")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestIntegrationProcessMiddlewareWrapsHandler(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "middleware"
	client := env.newClient(t)

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(500*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithProcessMiddleware(
			func(next Handler) Handler {
				return func(ctx context.Context, task *Task) error {
					ctx = context.WithValue(ctx, contextKey("trace"), "middleware")
					return next(ctx, task)
				}
			},
			func(next Handler) Handler {
				return func(ctx context.Context, task *Task) error {
					payload, ok := ctx.Value(contextKey("trace")).(string)
					if !ok || payload != "middleware" {
						return errors.New("missing middleware context")
					}

					return next(ctx, task)
				}
			},
		),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	done := make(chan struct{}, 1)
	worker.Handle("jobs.middleware", func(ctx context.Context, task *Task) error {
		if ctx.Value(contextKey("trace")) != "middleware" {
			return errors.New("handler did not receive middleware context")
		}

		done <- struct{}{}
		return nil
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "middleware"})
	require.NoError(t, err)
	task, err := NewTask("jobs.middleware", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for middleware-wrapped handler")
	}
}

func TestIntegrationMiddlewareCanWriteAndReadTaskHeaders(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	client, err := NewClient(
		env.js,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithDispatchMiddleware(func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				task.SetHeader("X-Request-ID", "req-42")
				return next(ctx, task, queue)
			}
		}),
	)
	require.NoError(t, err)

	worker, err := NewWorker(
		env.js,
		"emails",
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(500*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
		WithProcessMiddleware(func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				if task.Header("X-Request-ID") != "req-42" {
					return errors.New("missing task header")
				}

				return next(ctx, task)
			}
		}),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	done := make(chan struct{}, 1)
	worker.Handle("emails.send", func(ctx context.Context, task *Task) error {
		if task.Header("X-Request-ID") != "req-42" {
			return errors.New("handler did not receive task header")
		}

		done <- struct{}{}
		return nil
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "headers"})
	require.NoError(t, err)
	task, err := NewTask("emails.send", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, "emails"))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for header-aware middleware")
	}
}

func TestIntegrationUnhandledTaskIsTerminated(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "orphaned"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	body, err := json.Marshal(map[string]string{"kind": "orphan"})
	require.NoError(t, err)
	task, err := NewTask("jobs.unknown", body)
	require.NoError(t, err)

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	waitFor(t, 10*time.Second, func() bool {
		select {
		case err := <-workerErr:
			require.NoError(t, err)
		default:
		}

		return env.streamInfo(t).State.Msgs == 0
	})
}

func TestIntegrationMalformedMessageIsTerminated(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "malformed"
	worker := env.newWorker(t, queue)

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	msg := nats.NewMsg(queueSubject(env.subjectPrefix, queue))
	msg.Data = []byte(`{"status":"broken"}`)
	_, err := env.js.PublishMsg(context.Background(), msg)
	require.NoError(t, err)

	waitFor(t, 10*time.Second, func() bool {
		select {
		case err := <-workerErr:
			require.NoError(t, err)
		default:
		}

		return env.streamInfo(t).State.Msgs == 0
	})
}

func TestIntegrationWorkerRunStopsOnCanceledContext(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	worker := env.newWorker(t, "cancel")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for worker shutdown")
	}
}

func TestIntegrationWorkerGracefulShutdownWaitsForActiveHandler(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "graceful"
	client := env.newClient(t)
	worker := env.newWorker(t, queue)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finished := make(chan struct{}, 1)

	worker.Handle("jobs.graceful", func(ctx context.Context, task *Task) error {
		started <- struct{}{}
		<-release
		finished <- struct{}{}
		return nil
	})

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(runCtx)
	}()

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()

	body, err := json.Marshal(map[string]string{"kind": "shutdown"})
	require.NoError(t, err)
	task, err := NewTask("jobs.graceful", body)
	require.NoError(t, err)

	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-started:
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for handler start")
	}

	cancel()

	select {
	case err := <-errCh:
		require.FailNow(t, "worker stopped before active handler finished: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)

	select {
	case <-finished:
	case err := <-errCh:
		require.NoError(t, err)
		require.FailNow(t, "worker stopped before handler completion signal")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for handler completion")
	}

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for graceful worker shutdown")
	}
}

func TestIntegrationLongRunningHandlerKeepsDeliveryLease(t *testing.T) {
	t.Parallel()

	env := newIntegrationEnv(t)
	queue := "lease"
	client := env.newClient(t)

	worker, err := NewWorker(
		env.js,
		queue,
		WithStreamName(env.streamName),
		WithSubjectPrefix(env.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithAckWait(500*time.Millisecond),
		WithProgressInterval(100*time.Millisecond),
		WithFetchTimeout(200*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
	)
	require.NoError(t, err)
	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	worker.Handle("jobs.long", func(ctx context.Context, task *Task) error {
		attempts.Add(1)
		time.Sleep(1200 * time.Millisecond)
		done <- struct{}{}
		return nil
	})

	workerErr, stopWorker := runWorker(t, worker)
	defer stopWorker()

	body, err := json.Marshal(map[string]string{"kind": "long"})
	require.NoError(t, err)
	task, err := NewTask("jobs.long", body)
	require.NoError(t, err)

	dispatchCtx, cancelDispatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDispatch()
	require.NoError(t, client.Dispatch(dispatchCtx, task, queue))

	select {
	case <-done:
	case err := <-workerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout waiting for long-running task")
	}

	require.Equal(t, int32(1), attempts.Load())

	waitFor(t, 5*time.Second, func() bool {
		return env.streamInfo(t).State.Msgs == 0
	})
}

type integrationEnv struct {
	nc            *nats.Conn
	js            jetstream.JetStream
	streamName    string
	subjectPrefix string
}

func newIntegrationEnv(t *testing.T) integrationEnv {
	t.Helper()

	nc := mustNATSConn(t)
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	require.NoError(t, err)

	env := integrationEnv{
		nc:            nc,
		js:            js,
		streamName:    testToken(t, "stream"),
		subjectPrefix: testToken(t, "subject"),
	}

	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), env.streamName)
	})

	return env
}

func (e integrationEnv) newClient(t *testing.T) *Client {
	t.Helper()

	client, err := NewClient(
		e.js,
		WithStreamName(e.streamName),
		WithSubjectPrefix(e.subjectPrefix),
	)
	require.NoError(t, err)

	return client
}

func (e integrationEnv) newWorker(t *testing.T, queue string) *Worker {
	t.Helper()

	worker, err := NewWorker(
		e.js,
		queue,
		WithStreamName(e.streamName),
		WithSubjectPrefix(e.subjectPrefix),
		WithConsumerPrefix(testToken(t, "consumer")),
		WithFetchTimeout(500*time.Millisecond),
		WithIdleWait(10*time.Millisecond),
	)
	require.NoError(t, err)

	worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	return worker
}

func (e integrationEnv) streamInfo(t *testing.T) *jetstream.StreamInfo {
	t.Helper()

	stream, err := e.js.Stream(context.Background(), e.streamName)
	require.NoError(t, err)

	info, err := stream.Info(context.Background())
	require.NoError(t, err)

	return info
}

func (e integrationEnv) consumerInfo(t *testing.T, name string) *jetstream.ConsumerInfo {
	t.Helper()

	consumer, err := e.js.Consumer(context.Background(), e.streamName, name)
	require.NoError(t, err)

	info, err := consumer.Info(context.Background())
	require.NoError(t, err)

	return info
}

func (e integrationEnv) inspectConsumer(t *testing.T, subject string) jetstream.Consumer {
	t.Helper()

	name := testToken(t, "inspect")
	consumer, err := e.js.CreateOrUpdateConsumer(
		context.Background(),
		e.streamName,
		jetstream.ConsumerConfig{
			Durable:       name,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: subject,
		},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = e.js.DeleteConsumer(context.Background(), e.streamName, name)
	})

	return consumer
}

func runWorker(t *testing.T, worker *Worker) (<-chan error, func()) {
	t.Helper()

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerErr := make(chan error, 1)

	go func() {
		workerErr <- worker.Run(workerCtx)
	}()

	stop := func() {
		cancelWorker()
		select {
		case err := <-workerErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "timeout waiting for worker shutdown")
		}
	}

	return workerErr, stop
}

func mustFetchOne(t *testing.T, consumer jetstream.Consumer, timeout time.Duration) jetstream.Msg {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msgs, err := consumer.Fetch(1, jetstream.FetchContext(ctx))
	require.NoError(t, err)

	var got []jetstream.Msg
	for msg := range msgs.Messages() {
		got = append(got, msg)
	}

	require.NoError(t, msgs.Error())
	require.Len(t, got, 1)

	return got[0]
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	require.FailNow(t, "condition was not met before timeout")
}

func mustNATSConn(t *testing.T) *nats.Conn {
	t.Helper()

	url := os.Getenv(testNATSURLKey)
	if url == "" {
		t.Skipf("%s is not set", testNATSURLKey)
	}

	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	require.NoError(t, err)

	return nc
}

func testToken(t *testing.T, prefix string) string {
	t.Helper()

	return sanitizeToken(fmt.Sprintf("%s-%s-%d", prefix, t.Name(), time.Now().UnixNano()))
}
