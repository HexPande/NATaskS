package natasks

import (
	"encoding/json"
	"testing"

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

func TestNewTaskRequiresName(t *testing.T) {
	task, err := NewTask("", []byte(`{}`))
	require.Nil(t, task)
	require.ErrorIs(t, err, ErrEmptyTaskName)
}

func TestTaskNilBehavior(t *testing.T) {
	var task *Task
	require.Equal(t, "", task.Name())
	require.Nil(t, task.Payload())
	require.Equal(t, "", task.MessageID())
	require.Nil(t, task.WithMessageID("job-42"))
	require.EqualError(t, task.Unmarshal(&struct{}{}), "natasks: nil task")
}

func TestTaskUnmarshalError(t *testing.T) {
	task, err := NewTask("emails.send", []byte(`{`))
	require.NoError(t, err)

	err = task.Unmarshal(&struct{}{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal task payload")
}
