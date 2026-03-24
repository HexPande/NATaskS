package natasks

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

var sanitizeTokenReplacer = strings.NewReplacer(".", "-", " ", "-", ">", "-", "*", "-")

func (w *Worker) handler(name string) (Handler, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	handler, ok := w.handlers[name]
	return handler, ok
}

func (w *Worker) ensureConsumer() (jetstream.Consumer, error) {
	ctx, cancel := managementContext()
	defer cancel()

	consumer, err := w.js.CreateConsumer(ctx, w.cfg.streamName, w.consumerConfig())
	if err == nil {
		return consumer, nil
	}

	if !errors.Is(err, jetstream.ErrConsumerExists) {
		return nil, fmt.Errorf("natasks: add consumer: %w", err)
	}

	consumer, infoErr := w.js.Consumer(ctx, w.cfg.streamName, w.consumerName())
	if infoErr == nil {
		return consumer, nil
	}

	return nil, fmt.Errorf("natasks: add consumer: %w", err)
}

func (w *Worker) consumerConfig() jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       w.consumerName(),
		Description:   "natasks worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.cfg.ackWait,
		FilterSubject: queueSubject(w.cfg.subjectPrefix, w.queue),
		MaxAckPending: w.cfg.maxAckPending,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}
}

func (w *Worker) consumerName() string {
	if w.cfg.durable != "" {
		return w.cfg.durable
	}

	return w.cfg.consumerPrefix + "-" + sanitizeToken(w.queue)
}

func sanitizeToken(in string) string {
	return sanitizeTokenReplacer.Replace(strings.TrimSpace(in))
}
