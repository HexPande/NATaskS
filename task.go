package natasks

import (
	"encoding/json"
	"errors"
	"fmt"
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
}

// NewTask constructs a task with a raw payload.
func NewTask(name string, payload []byte) (*Task, error) {
	if name == "" {
		return nil, ErrEmptyTaskName
	}

	cloned := append([]byte(nil), payload...)

	return &Task{
		name:    name,
		payload: cloned,
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
