package natasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (w *Worker) startProgressLoop(msg jetstream.Msg) func() {
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(w.cfg.progressInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					w.logger.Warn("natasks: mark message in progress", "error", err)
					return
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (w *Worker) consumerForRun() (jetstream.Consumer, error) {
	if w.consumer != nil {
		return w.consumer, nil
	}

	consumer, err := w.js.Consumer(context.Background(), w.cfg.streamName, w.consumerName())
	if err != nil {
		ctx, cancel := managementContext()
		defer cancel()
		consumer, err = w.js.Consumer(ctx, w.cfg.streamName, w.consumerName())
	}
	if err != nil {
		return nil, err
	}

	w.consumer = consumer
	return consumer, nil
}

func (w *Worker) fetchBatch(ctx context.Context, consumer jetstream.Consumer) ([]jetstream.Msg, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, w.cfg.fetchTimeout)
	defer cancel()

	batch, err := consumer.Fetch(w.fetchSize(), jetstream.FetchContext(fetchCtx))
	if err != nil {
		return nil, err
	}

	var msgs []jetstream.Msg
	for msg := range batch.Messages() {
		msgs = append(msgs, msg)
	}

	return msgs, batch.Error()
}

func (w *Worker) fetchSize() int {
	if w.cfg.concurrency > w.cfg.fetchBatch {
		return w.cfg.concurrency
	}

	return w.cfg.fetchBatch
}

func (w *Worker) handleFetchResult(ctx context.Context, err error) (stop bool, runErr error) {
	switch {
	case err == nil:
		return false, nil
	case w.isShutdownFetchError(ctx, err):
		return true, nil
	case w.isIdleFetchError(ctx, err):
		w.waitAfterIdleFetch()
		return false, nil
	default:
		return true, fmt.Errorf("natasks: fetch messages: %w", err)
	}
}

func (w *Worker) waitAfterIdleFetch() {
	time.Sleep(w.cfg.idleWait)
}

func (w *Worker) isIdleFetchError(ctx context.Context, err error) bool {
	if errors.Is(err, nats.ErrTimeout) {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ctx.Err() == nil
	}

	return false
}

func (w *Worker) isShutdownFetchError(ctx context.Context, err error) bool {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return ctx.Err() != nil
}

func (w *Worker) terminateMessage(msg jetstream.Msg, reason string, cause error) error {
	if err := msg.Term(); err != nil {
		return fmt.Errorf("natasks: terminate %s message: %w", reason, err)
	}

	return cause
}
