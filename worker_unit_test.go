package natasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

type testPropagator struct {
	value string
}

func (p testPropagator) Inject(ctx context.Context, carrier TextMapCarrier) {}

func (p testPropagator) Extract(ctx context.Context, carrier TextMapCarrier) context.Context {
	return context.WithValue(ctx, contextKey("trace"), p.value)
}

type testMsg struct {
	headers   nats.Header
	data      []byte
	meta      *jetstream.MsgMetadata
	ackErr    error
	nakErr    error
	termErr   error
	inProgErr error
	nakDelay  time.Duration
	acked     bool
	nacked    bool
	termed    bool
	progress  bool
}

func (m *testMsg) Metadata() (*jetstream.MsgMetadata, error) { return m.meta, nil }
func (m *testMsg) Data() []byte                              { return m.data }
func (m *testMsg) Headers() nats.Header                      { return m.headers }
func (m *testMsg) Subject() string                           { return "" }
func (m *testMsg) Reply() string                             { return "" }
func (m *testMsg) Ack() error                                { m.acked = true; return m.ackErr }
func (m *testMsg) DoubleAck(context.Context) error           { m.acked = true; return m.ackErr }
func (m *testMsg) Nak() error                                { m.nacked = true; return m.nakErr }
func (m *testMsg) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	m.nakDelay = delay
	return m.nakErr
}
func (m *testMsg) InProgress() error           { m.progress = true; return m.inProgErr }
func (m *testMsg) Term() error                 { m.termed = true; return m.termErr }
func (m *testMsg) TermWithReason(string) error { m.termed = true; return m.termErr }

func TestWorkerProcessingContext(t *testing.T) {
	w := &Worker{cfg: workerConfig{taskTimeout: 0}}
	ctx, cancel := w.processingContext(context.Background())
	defer cancel()
	require.NotNil(t, ctx)

	w.cfg.taskTimeout = 10 * time.Millisecond
	ctx, cancel = w.processingContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(10*time.Millisecond), deadline, 20*time.Millisecond)
}

func TestWorkerRunHandlerRecovery(t *testing.T) {
	w := &Worker{}
	task, err := NewTask("jobs.panic", []byte(`{}`))
	require.NoError(t, err)

	err = w.runHandler(context.Background(), func(ctx context.Context, task *Task) error {
		panic("boom")
	}, task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic recovered: boom")
}

func TestWorkerMessageTask(t *testing.T) {
	w := &Worker{
		handlers: map[string]Handler{
			"jobs.test": func(context.Context, *Task) error { return nil },
		},
	}

	msg := &testMsg{
		headers: nats.Header{
			headerTaskName:        []string{"jobs.test"},
			jetstream.MsgIDHeader: []string{"job-42"},
		},
		data: []byte(`{}`),
	}

	task, handler, err := w.messageTask(msg)
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.Equal(t, "job-42", task.MessageID())
}

func TestWorkerMessageTaskErrors(t *testing.T) {
	w := &Worker{handlers: map[string]Handler{}}

	_, _, err := w.messageTask(&testMsg{headers: nats.Header{}})
	require.Error(t, err)

	_, _, err = w.messageTask(&testMsg{headers: nats.Header{headerTaskName: []string{"jobs.unknown"}}})
	require.ErrorIs(t, err, ErrHandlerNotFound)
}

func TestWorkerAckMessage(t *testing.T) {
	w := &Worker{}
	msg := &testMsg{}
	require.NoError(t, w.ackMessage(msg))
	require.True(t, msg.acked)

	msg = &testMsg{ackErr: errors.New("ack failed")}
	require.Error(t, w.ackMessage(msg))
}

func TestWorkerHandleMessageSuccess(t *testing.T) {
	w := &Worker{
		cfg: workerConfig{
			config: config{
				propagator: testPropagator{value: "ok"},
			},
			processMiddleware: nil,
			progressInterval:  time.Hour,
		},
		handlers: map[string]Handler{
			"jobs.test": func(ctx context.Context, task *Task) error {
				require.Equal(t, "ok", ctx.Value(contextKey("trace")))
				return nil
			},
		},
	}

	msg := &testMsg{
		headers: nats.Header{
			headerTaskName: []string{"jobs.test"},
		},
		data: []byte(`{}`),
	}

	require.NoError(t, w.handleMessage(context.Background(), msg))
	require.True(t, msg.acked)
}

func TestWorkerHandleMessageRetry(t *testing.T) {
	w := &Worker{
		cfg: workerConfig{
			progressInterval: time.Hour,
			maxRetries:       -1,
			retryBackoff:     []time.Duration{time.Second},
		},
		handlers: map[string]Handler{
			"jobs.test": func(ctx context.Context, task *Task) error {
				return errors.New("retry me")
			},
		},
	}

	msg := &testMsg{
		headers: nats.Header{
			headerTaskName: []string{"jobs.test"},
		},
		data: []byte(`{}`),
		meta: &jetstream.MsgMetadata{NumDelivered: 1},
	}

	err := w.handleMessage(context.Background(), msg)
	require.EqualError(t, err, "retry me")
	require.True(t, msg.nacked)
	require.Equal(t, time.Second, msg.nakDelay)
}

func TestWorkerHandleFetchResult(t *testing.T) {
	w := &Worker{cfg: workerConfig{idleWait: 0}}

	stop, err := w.handleFetchResult(context.Background(), nil)
	require.False(t, stop)
	require.NoError(t, err)

	stop, err = w.handleFetchResult(context.Background(), nats.ErrTimeout)
	require.False(t, stop)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stop, err = w.handleFetchResult(ctx, context.Canceled)
	require.True(t, stop)
	require.NoError(t, err)

	stop, err = w.handleFetchResult(context.Background(), errors.New("boom"))
	require.True(t, stop)
	require.Error(t, err)
}

func TestWorkerRetryHelpers(t *testing.T) {
	w := &Worker{cfg: workerConfig{maxRetries: 2, retryBackoff: []time.Duration{time.Second, 2 * time.Second}}}
	require.False(t, w.exhaustedRetries(1))
	require.True(t, w.exhaustedRetries(3))
	require.Equal(t, time.Second, w.redeliveryDelay(1))
	require.Equal(t, 2*time.Second, w.redeliveryDelay(5))
}

func TestWorkerAckDeadLetteredMessage(t *testing.T) {
	w := &Worker{}
	msg := &testMsg{}
	handlerErr := errors.New("handler failed")
	require.Equal(t, handlerErr, w.ackDeadLetteredMessage(msg, handlerErr))
	require.True(t, msg.acked)
}
