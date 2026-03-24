package otel

import (
	"context"

	"github.com/hexpande/natasks"
	gootel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const defaultInstrumentationName = "github.com/hexpande/natasks"

type Options struct {
	TracerProvider      trace.TracerProvider
	Propagator          propagation.TextMapPropagator
	InstrumentationName string
}

type Middleware struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

var _ natasks.MessagePropagator = (*Middleware)(nil)

func New(opts Options) *Middleware {
	provider := opts.TracerProvider
	if provider == nil {
		provider = gootel.GetTracerProvider()
	}
	propagator := opts.Propagator
	if propagator == nil {
		propagator = gootel.GetTextMapPropagator()
	}
	name := opts.InstrumentationName
	if name == "" {
		name = defaultInstrumentationName
	}
	return &Middleware{tracer: provider.Tracer(name), propagator: propagator}
}

func (m *Middleware) Inject(ctx context.Context, carrier natasks.TextMapCarrier) {
	if m == nil {
		return
	}
	m.propagator.Inject(ctx, carrier)
}

func (m *Middleware) Extract(ctx context.Context, carrier natasks.TextMapCarrier) context.Context {
	if m == nil {
		return ctx
	}
	return m.propagator.Extract(ctx, carrier)
}

func (m *Middleware) DispatchMiddleware() natasks.DispatchMiddleware {
	if m == nil {
		return nil
	}
	return func(next natasks.DispatchFunc) natasks.DispatchFunc {
		return func(ctx context.Context, task *natasks.Task, queue string) error {
			ctx, span := m.tracer.Start(ctx, "natasks dispatch",
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					attribute.String("messaging.system", "nats"),
					attribute.String("messaging.operation", "publish"),
					attribute.String("messaging.destination.name", queue),
					attribute.String("natasks.task", task.Name()),
				),
			)
			defer span.End()
			err := next(ctx, task, queue)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}

func (m *Middleware) ProcessMiddleware(queue string) natasks.ProcessMiddleware {
	if m == nil {
		return nil
	}
	return func(next natasks.Handler) natasks.Handler {
		return func(ctx context.Context, task *natasks.Task) error {
			ctx, span := m.tracer.Start(ctx, "natasks process",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "nats"),
					attribute.String("messaging.operation", "process"),
					attribute.String("messaging.destination.name", queue),
					attribute.String("natasks.task", task.Name()),
				),
			)
			defer span.End()
			err := next(ctx, task)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}
