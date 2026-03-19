package natasks

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestBuildTaskMessage(t *testing.T) {
	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)
	task.WithMessageID("job-42")
	task.SetHeader("X-Trace-ID", "trace-42")

	msg := buildTaskMessage("app.tasks.emails", "emails", task)

	require.Equal(t, "app.tasks.emails", msg.Subject)
	require.Equal(t, "jobs.test", msg.Header.Get(headerTaskName))
	require.Equal(t, "emails", msg.Header.Get(headerQueueName))
	require.Equal(t, "job-42", msg.Header.Get(jetstream.MsgIDHeader))
	require.Equal(t, "trace-42", msg.Header.Get("X-Trace-ID"))
	require.Equal(t, []byte(`{}`), msg.Data)
}

func TestTaskFromMessage(t *testing.T) {
	msg := &testMsg{
		headers: nats.Header{
			headerTaskName:        []string{"jobs.test"},
			headerQueueName:       []string{"emails"},
			jetstream.MsgIDHeader: []string{"job-42"},
			"X-Trace-ID":          []string{"trace-42"},
		},
		data: []byte(`{}`),
	}

	task, err := taskFromMessage(msg)
	require.NoError(t, err)
	require.Equal(t, "jobs.test", task.Name())
	require.Equal(t, "job-42", task.MessageID())
	require.Equal(t, "trace-42", task.Header("X-Trace-ID"))

	headers := task.Headers()
	headers.Set("X-Trace-ID", "changed")
	require.Equal(t, "trace-42", task.Header("X-Trace-ID"))
}

func TestTaskFromMessageRequiresTaskHeader(t *testing.T) {
	_, err := taskFromMessage(&testMsg{headers: nats.Header{}, data: []byte(`{}`)})
	require.EqualError(t, err, "natasks: message missing task name header")
}
