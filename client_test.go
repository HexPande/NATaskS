package natasks

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestClientDispatchTaskValidation(t *testing.T) {
	var client *Client
	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)

	require.EqualError(t, client.Dispatch(context.Background(), task, "jobs"), "natasks: nil client")

	client = &Client{
		dispatch: func(ctx context.Context, task *Task, queue string) error { return nil },
	}

	require.EqualError(t, client.Dispatch(context.Background(), nil, "jobs"), "natasks: nil task")
	require.EqualError(t, client.Dispatch(context.Background(), task, internalScheduleToken), `natasks: queue "__natasks_schedule" is reserved for internal scheduling`)
}

func TestClientDispatchNormalizesNilContext(t *testing.T) {
	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)

	var gotCtx context.Context
	client := &Client{
		dispatch: func(ctx context.Context, task *Task, queue string) error {
			gotCtx = ctx
			return nil
		},
	}

	require.NoError(t, client.Dispatch(nil, task, "jobs"))
	require.NotNil(t, gotCtx)
}

func TestClientNewMessage(t *testing.T) {
	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)
	task.WithMessageID("job-42")

	client := &Client{cfg: config{subjectPrefix: "app.tasks"}}
	msg := client.newMessage(context.Background(), "emails", task)

	require.Equal(t, "app.tasks.emails", msg.Subject)
	require.Equal(t, "jobs.test", msg.Header.Get(headerTaskName))
	require.Equal(t, "emails", msg.Header.Get(headerQueueName))
	require.Equal(t, "job-42", msg.Header.Get(jetstream.MsgIDHeader))
}

func TestClientNewScheduledMessage(t *testing.T) {
	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)

	client := &Client{cfg: config{subjectPrefix: "app.tasks"}}
	at := time.Date(2026, 3, 18, 10, 0, 0, 0, time.UTC)
	msg := client.newMessage(withDispatchScheduleAt(context.Background(), at), "emails", task)

	require.Equal(t, "app.tasks.__natasks_schedule", msg.Subject)
	require.Equal(t, "@at 2026-03-18T10:00:00Z", msg.Header.Get(headerSchedule))
	require.Equal(t, "app.tasks.emails", msg.Header.Get(headerScheduleTarget))
}

func TestDispatchScheduleAt(t *testing.T) {
	at := time.Now().UTC().Add(time.Minute).Round(0)
	got, ok := dispatchScheduleAt(withDispatchScheduleAt(nil, at))
	require.True(t, ok)
	require.Equal(t, at, got)
}
