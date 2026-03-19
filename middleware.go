package natasks

import "context"

// TextMapCarrier is a minimal carrier interface for context propagation.
type TextMapCarrier interface {
	Get(string) string
	Set(string, string)
	Keys() []string
}

// MessagePropagator injects and extracts context values into message headers.
type MessagePropagator interface {
	Inject(context.Context, TextMapCarrier)
	Extract(context.Context, TextMapCarrier) context.Context
}

// DispatchFunc publishes a task to a queue.
type DispatchFunc func(context.Context, *Task, string) error

// DispatchMiddleware wraps task publishing.
type DispatchMiddleware func(DispatchFunc) DispatchFunc

// ProcessMiddleware wraps task processing.
type ProcessMiddleware func(Handler) Handler

func chainDispatchMiddleware(final DispatchFunc, middlewares []DispatchMiddleware) DispatchFunc {
	chained := final
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] == nil {
			continue
		}

		chained = middlewares[i](chained)
	}

	return chained
}

func chainProcessMiddleware(final Handler, middlewares []ProcessMiddleware) Handler {
	chained := final
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] == nil {
			continue
		}

		chained = middlewares[i](chained)
	}

	return chained
}
