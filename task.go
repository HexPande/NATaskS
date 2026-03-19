package natasks

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

const (
	headerTaskName       = "Natasks-Task"
	headerQueueName      = "Natasks-Queue"
	headerOriginalQueue  = "Natasks-Original-Queue"
	headerAttempts       = "Natasks-Attempts"
	headerLastError      = "Natasks-Last-Error"
	headerSchedule       = "Nats-Schedule"
	headerScheduleTarget = "Nats-Schedule-Target"
)

var ErrEmptyTaskName = errors.New("natasks: task name is required")

// Task is a serializable unit of work.
// It intentionally mirrors the simple "type + payload" model used by queue
// systems such as Laravel queue jobs and asynq tasks.
type Task struct {
	name      string
	payload   []byte
	messageID string
	headers   nats.Header
}

// NewTask constructs a task with a raw payload.
func NewTask(name string, payload []byte) (*Task, error) {
	return newTask(name, payload, nil, "", true)
}

func newTask(name string, payload []byte, headers nats.Header, messageID string, clone bool) (*Task, error) {
	if name == "" {
		return nil, ErrEmptyTaskName
	}

	if clone {
		payload = append([]byte(nil), payload...)
		headers = cloneHeaders(headers)
	}

	return &Task{
		name:      name,
		payload:   payload,
		messageID: messageID,
		headers:   headers,
	}, nil
}

// Name returns the task name.
func (t *Task) Name() string {
	if t == nil {
		return ""
	}

	return t.name
}

// Payload returns a copy of the raw payload.
func (t *Task) Payload() []byte {
	if t == nil {
		return nil
	}

	return append([]byte(nil), t.payload...)
}

// Header returns the first value for the given task header key.
func (t *Task) Header(key string) string {
	if t == nil || t.headers == nil {
		return ""
	}

	return t.headers.Get(key)
}

// Headers returns a copy of task headers.
func (t *Task) Headers() nats.Header {
	if t == nil {
		return nil
	}

	return cloneHeaders(t.headers)
}

// SetHeader sets a task header value.
func (t *Task) SetHeader(key, value string) *Task {
	if t == nil {
		return nil
	}

	if t.headers == nil {
		t.headers = nats.Header{}
	}
	t.headers.Set(key, value)
	return t
}

// AddHeader appends a task header value.
func (t *Task) AddHeader(key, value string) *Task {
	if t == nil {
		return nil
	}

	if t.headers == nil {
		t.headers = nats.Header{}
	}
	t.headers.Add(key, value)
	return t
}

// WithMessageID sets the JetStream message ID used for publish deduplication.
func (t *Task) WithMessageID(id string) *Task {
	if t == nil {
		return nil
	}

	t.messageID = id
	return t
}

// MessageID returns the configured JetStream message ID.
func (t *Task) MessageID() string {
	if t == nil {
		return ""
	}

	return t.messageID
}

// Unmarshal decodes the raw payload into dst.
func (t *Task) Unmarshal(dst any) error {
	if t == nil {
		return errors.New("natasks: nil task")
	}

	if err := json.Unmarshal(t.payload, dst); err != nil {
		return fmt.Errorf("natasks: unmarshal task payload: %w", err)
	}

	return nil
}

func cloneHeaders(headers nats.Header) nats.Header {
	if len(headers) == 0 {
		return nil
	}

	cloned := make(nats.Header, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}

	return cloned
}
