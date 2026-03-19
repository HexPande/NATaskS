package natasks

import (
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestTaskJSONRoundtrip(t *testing.T) {
	type payload struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	body, err := json.Marshal(payload{
		ID:   42,
		Name: "welcome",
	})
	require.NoError(t, err)

	task, err := NewTask("emails.send", body)
	require.NoError(t, err)

	var got payload
	require.NoError(t, task.Unmarshal(&got))
	require.Equal(t, payload{ID: 42, Name: "welcome"}, got)
}

func TestNormalizeQueue(t *testing.T) {
	require.Equal(t, "default", normalizeQueue(""))
	require.Equal(t, "natasks.emails", queueSubject("natasks", "emails"))
}

func TestTaskMessageID(t *testing.T) {
	task, err := NewTask("emails.send", []byte(`{}`))
	require.NoError(t, err)

	require.Equal(t, "", task.MessageID())
	require.Same(t, task, task.WithMessageID("job-42"))
	require.Equal(t, "job-42", task.MessageID())
}

func TestTaskHeaders(t *testing.T) {
	task, err := NewTask("emails.send", []byte(`{}`))
	require.NoError(t, err)

	require.Same(t, task, task.SetHeader("X-Request-ID", "req-1"))
	require.Same(t, task, task.AddHeader("X-Role", "user"))
	require.Same(t, task, task.AddHeader("X-Role", "admin"))

	require.Equal(t, "req-1", task.Header("X-Request-ID"))
	require.Equal(t, []string{"user", "admin"}, task.Headers()["X-Role"])

	headers := task.Headers()
	headers.Set("X-Request-ID", "changed")
	require.Equal(t, "req-1", task.Header("X-Request-ID"))
}

func TestNewTaskRequiresName(t *testing.T) {
	task, err := NewTask("", []byte(`{}`))
	require.Nil(t, task)
	require.ErrorIs(t, err, ErrEmptyTaskName)
}

func TestTaskNilBehavior(t *testing.T) {
	var task *Task
	require.Equal(t, "", task.Name())
	require.Nil(t, task.Payload())
	require.Equal(t, "", task.Header("X-Test"))
	require.Nil(t, task.Headers())
	require.Equal(t, "", task.MessageID())
	require.Nil(t, task.WithMessageID("job-42"))
	require.Nil(t, task.SetHeader("X-Test", "value"))
	require.Nil(t, task.AddHeader("X-Test", "value"))
	require.EqualError(t, task.Unmarshal(&struct{}{}), "natasks: nil task")
}

func TestCloneHeaders(t *testing.T) {
	headers := nats.Header{
		"X-Test": []string{"a", "b"},
	}

	cloned := cloneHeaders(headers)
	require.Equal(t, headers, cloned)

	cloned.Add("X-Test", "c")
	require.Equal(t, []string{"a", "b"}, headers["X-Test"])
}

func TestTaskUnmarshalError(t *testing.T) {
	task, err := NewTask("emails.send", []byte(`{`))
	require.NoError(t, err)

	err = task.Unmarshal(&struct{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal task payload")
}
