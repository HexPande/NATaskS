package prometheus

import (
	"context"
	"errors"
	"time"

	"github.com/hexpande/natasks"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricStatusSuccess = "success"
	metricStatusError   = "error"
)

type Options struct {
	Registerer prometheus.Registerer
	Namespace  string
	Subsystem  string

	DispatchDurationBuckets []float64
	ProcessDurationBuckets  []float64
}

type Metrics struct {
	dispatchTotal    *prometheus.CounterVec
	dispatchDuration *prometheus.HistogramVec
	processTotal     *prometheus.CounterVec
	processDuration  *prometheus.HistogramVec
	processInflight  *prometheus.GaugeVec
}

func New(opts Options) (*Metrics, error) {
	registerer := opts.Registerer
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	dispatchTotal, err := registerCounterVec(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: opts.Namespace, Subsystem: opts.Subsystem, Name: "dispatch_total", Help: "Total number of natasks dispatch attempts."},
		[]string{"queue", "task", "status"},
	))
	if err != nil {
		return nil, err
	}

	dispatchDuration, err := registerHistogramVec(registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: opts.Namespace, Subsystem: opts.Subsystem, Name: "dispatch_duration_seconds", Help: "Duration of natasks dispatch attempts.", Buckets: withDefaultBuckets(opts.DispatchDurationBuckets)},
		[]string{"queue", "task", "status"},
	))
	if err != nil {
		return nil, err
	}

	processTotal, err := registerCounterVec(registerer, prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: opts.Namespace, Subsystem: opts.Subsystem, Name: "process_total", Help: "Total number of natasks processing attempts."},
		[]string{"queue", "task", "status"},
	))
	if err != nil {
		return nil, err
	}

	processDuration, err := registerHistogramVec(registerer, prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: opts.Namespace, Subsystem: opts.Subsystem, Name: "process_duration_seconds", Help: "Duration of natasks processing attempts.", Buckets: withDefaultBuckets(opts.ProcessDurationBuckets)},
		[]string{"queue", "task", "status"},
	))
	if err != nil {
		return nil, err
	}

	processInflight, err := registerGaugeVec(registerer, prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: opts.Namespace, Subsystem: opts.Subsystem, Name: "process_inflight", Help: "Current number of natasks being processed."},
		[]string{"queue", "task"},
	))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		dispatchTotal:    dispatchTotal,
		dispatchDuration: dispatchDuration,
		processTotal:     processTotal,
		processDuration:  processDuration,
		processInflight:  processInflight,
	}, nil
}

func (m *Metrics) DispatchMiddleware() natasks.DispatchMiddleware {
	if m == nil {
		return nil
	}

	return func(next natasks.DispatchFunc) natasks.DispatchFunc {
		return func(ctx context.Context, task *natasks.Task, queue string) error {
			start := time.Now()
			err := next(ctx, task, queue)
			status := metricStatusSuccess
			if err != nil {
				status = metricStatusError
			}
			m.dispatchTotal.WithLabelValues(queue, task.Name(), status).Inc()
			m.dispatchDuration.WithLabelValues(queue, task.Name(), status).Observe(time.Since(start).Seconds())
			return err
		}
	}
}

func (m *Metrics) ProcessMiddleware(queue string) natasks.ProcessMiddleware {
	if m == nil {
		return nil
	}

	return func(next natasks.Handler) natasks.Handler {
		return func(ctx context.Context, task *natasks.Task) error {
			labels := []string{queue, task.Name()}
			start := time.Now()
			m.processInflight.WithLabelValues(labels...).Inc()
			defer m.processInflight.WithLabelValues(labels...).Dec()
			err := next(ctx, task)
			status := metricStatusSuccess
			if err != nil {
				status = metricStatusError
			}
			m.processTotal.WithLabelValues(queue, task.Name(), status).Inc()
			m.processDuration.WithLabelValues(queue, task.Name(), status).Observe(time.Since(start).Seconds())
			return err
		}
	}
}

func withDefaultBuckets(buckets []float64) []float64 {
	if len(buckets) == 0 {
		return prometheus.DefBuckets
	}
	return buckets
}

func registerCounterVec(registerer prometheus.Registerer, collector *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
		existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.CounterVec)
		if !ok {
			return nil, err
		}
		return existing, nil
	}
	return collector, nil
}

func registerHistogramVec(registerer prometheus.Registerer, collector *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
		existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.HistogramVec)
		if !ok {
			return nil, err
		}
		return existing, nil
	}
	return collector, nil
}

func registerGaugeVec(registerer prometheus.Registerer, collector *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, err
		}
		existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.GaugeVec)
		if !ok {
			return nil, err
		}
		return existing, nil
	}
	return collector, nil
}
