package natasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (w *Worker) retryOrDeadLetter(msg jetstream.Msg, task *Task, handlerErr error) error {
	if errors.Is(handlerErr, ErrNoRetry) {
		return w.ackNoRetryMessage(msg, handlerErr)
	}

	deliveries, err := deliveryAttempts(msg)
	if err != nil {
		return err
	}

	if w.exhaustedRetries(deliveries) {
		if err := w.publishDLQ(msg, task, deliveries, handlerErr); err != nil {
			return err
		}

		return w.ackDeadLetteredMessage(msg, handlerErr)
	}

	delay := w.redeliveryDelay(deliveries)
	if delay > 0 {
		if err := msg.NakWithDelay(delay); err != nil {
			return fmt.Errorf("natasks: handler error %v, nak with delay failed: %w", handlerErr, err)
		}

		return handlerErr
	}

	if err := msg.Nak(); err != nil {
		return fmt.Errorf("natasks: handler error %v, nak failed: %w", handlerErr, err)
	}

	return handlerErr
}

func (w *Worker) ackNoRetryMessage(msg jetstream.Msg, handlerErr error) error {
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("natasks: ack no-retry message: %w", err)
	}

	return unwrapNoRetry(handlerErr)
}

func (w *Worker) exhaustedRetries(deliveries uint64) bool {
	if w.cfg.maxRetries < 0 {
		return false
	}

	retriesUsed := int(deliveries) - 1
	return retriesUsed >= w.cfg.maxRetries
}

func (w *Worker) redeliveryDelay(deliveries uint64) time.Duration {
	if len(w.cfg.retryBackoff) == 0 {
		return 0
	}

	retryIndex := int(deliveries) - 1
	if retryIndex < 0 {
		retryIndex = 0
	}

	if retryIndex >= len(w.cfg.retryBackoff) {
		return w.cfg.retryBackoff[len(w.cfg.retryBackoff)-1]
	}

	return w.cfg.retryBackoff[retryIndex]
}

func (w *Worker) ackDeadLetteredMessage(msg jetstream.Msg, handlerErr error) error {
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("natasks: ack dead-lettered message: %w", err)
	}

	return handlerErr
}

func (w *Worker) publishDLQ(msg jetstream.Msg, task *Task, deliveries uint64, handlerErr error) error {
	dlq := dlqQueue(w.queue, w.cfg.dlqSuffix)
	dlqMsg := nats.NewMsg(queueSubject(w.cfg.subjectPrefix, dlq))
	dlqMsg.Data = task.Payload()

	for key, values := range msg.Headers() {
		for _, value := range values {
			dlqMsg.Header.Add(key, value)
		}
	}

	dlqMsg.Header.Set(headerQueueName, dlq)
	dlqMsg.Header.Set(headerOriginalQueue, w.queue)
	dlqMsg.Header.Set(headerAttempts, attemptsHeaderValue(deliveries))
	dlqMsg.Header.Set(headerLastError, handlerErr.Error())

	if _, err := w.js.PublishMsg(context.Background(), dlqMsg); err != nil {
		return fmt.Errorf("natasks: publish dlq message: %w", err)
	}

	return nil
}

func deliveryAttempts(msg jetstream.Msg) (uint64, error) {
	meta, err := msg.Metadata()
	if err != nil {
		return 0, fmt.Errorf("natasks: message metadata: %w", err)
	}

	return meta.NumDelivered, nil
}
