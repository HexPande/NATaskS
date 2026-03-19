package natasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainDispatchMiddlewareOrder(t *testing.T) {
	var steps []string
	final := func(ctx context.Context, task *Task, queue string) error {
		steps = append(steps, "final:"+queue)
		return nil
	}

	chained := chainDispatchMiddleware(final, []DispatchMiddleware{
		func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				steps = append(steps, "m1")
				return next(ctx, task, queue+"-a")
			}
		},
		nil,
		func(next DispatchFunc) DispatchFunc {
			return func(ctx context.Context, task *Task, queue string) error {
				steps = append(steps, "m2")
				return next(ctx, task, queue+"-b")
			}
		},
	})

	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, chained(context.Background(), task, "queue"))
	require.Equal(t, []string{"m1", "m2", "final:queue-a-b"}, steps)
}

func TestChainProcessMiddlewareOrder(t *testing.T) {
	var steps []string
	final := func(ctx context.Context, task *Task) error {
		steps = append(steps, "final")
		return nil
	}

	chained := chainProcessMiddleware(final, []ProcessMiddleware{
		func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				steps = append(steps, "m1")
				return next(ctx, task)
			}
		},
		nil,
		func(next Handler) Handler {
			return func(ctx context.Context, task *Task) error {
				steps = append(steps, "m2")
				return next(ctx, task)
			}
		},
	})

	task, err := NewTask("jobs.test", []byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, chained(context.Background(), task))
	require.Equal(t, []string{"m1", "m2", "final"}, steps)
}
