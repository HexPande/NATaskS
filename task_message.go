package natasks

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func buildTaskMessage(subject, queue string, task *Task) *nats.Msg {
	msg := nats.NewMsg(subject)
	copyTaskHeaders(msg.Header, task.headers)
	msg.Header.Set(headerTaskName, task.name)
	msg.Header.Set(headerQueueName, queue)
	if task.messageID != "" {
		msg.Header.Set(jetstream.MsgIDHeader, task.messageID)
	}
	msg.Data = task.payload
	return msg
}

func taskFromMessage(msg jetstream.Msg) (*Task, error) {
	taskName := msg.Headers().Get(headerTaskName)
	if taskName == "" {
		return nil, fmt.Errorf("natasks: message missing task name header")
	}

	return newTask(taskName, msg.Data(), msg.Headers(), msg.Headers().Get(jetstream.MsgIDHeader), true)
}

func copyTaskHeaders(dst, src nats.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
