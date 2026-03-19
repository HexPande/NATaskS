package natasks

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultStreamName     = "NATASKS"
	defaultSubjectPrefix  = "natasks"
	defaultConsumerPrefix = "natasks"
	defaultDLQSuffix      = "-dlq"
	defaultFetchBatchSize = 1
	defaultFetchTimeout   = 5 * time.Second
	defaultIdleWait       = 250 * time.Millisecond
	defaultAckWait        = 30 * time.Second
)

type config struct {
	streamName         string
	subjectPrefix      string
	dispatchMiddleware []DispatchMiddleware
	propagator         MessagePropagator
}

type workerConfig struct {
	config
	consumerPrefix    string
	durable           string
	concurrency       int
	fetchBatch        int
	fetchTimeout      time.Duration
	taskTimeout       time.Duration
	idleWait          time.Duration
	ackWait           time.Duration
	progressInterval  time.Duration
	maxAckPending     int
	maxRetries        int
	retryBackoff      []time.Duration
	dlqSuffix         string
	processMiddleware []ProcessMiddleware
}

func defaultConfig() config {
	return config{
		streamName:    defaultStreamName,
		subjectPrefix: defaultSubjectPrefix,
	}
}

func (c config) validate() error {
	if strings.TrimSpace(c.streamName) == "" {
		return fmt.Errorf("natasks: stream name is required")
	}

	if strings.TrimSpace(c.subjectPrefix) == "" {
		return fmt.Errorf("natasks: subject prefix is required")
	}

	if strings.Contains(c.subjectPrefix, "*") || strings.Contains(c.subjectPrefix, ">") {
		return fmt.Errorf("natasks: subject prefix must not contain wildcards")
	}

	return nil
}

func defaultWorkerConfig() workerConfig {
	base := defaultConfig()

	return workerConfig{
		config:           base,
		consumerPrefix:   defaultConsumerPrefix,
		concurrency:      1,
		fetchBatch:       defaultFetchBatchSize,
		fetchTimeout:     defaultFetchTimeout,
		idleWait:         defaultIdleWait,
		ackWait:          defaultAckWait,
		progressInterval: defaultAckWait / 3,
		maxAckPending:    128,
		maxRetries:       -1,
		dlqSuffix:        defaultDLQSuffix,
	}
}

func (c workerConfig) validate() error {
	if err := c.config.validate(); err != nil {
		return err
	}

	if c.fetchBatch <= 0 {
		return fmt.Errorf("natasks: fetch batch must be positive")
	}

	if c.concurrency <= 0 {
		return fmt.Errorf("natasks: concurrency must be positive")
	}

	if c.fetchTimeout <= 0 {
		return fmt.Errorf("natasks: fetch timeout must be positive")
	}

	if c.taskTimeout < 0 {
		return fmt.Errorf("natasks: task timeout must not be negative")
	}

	if c.idleWait < 0 {
		return fmt.Errorf("natasks: idle wait must not be negative")
	}

	if c.ackWait <= 0 {
		return fmt.Errorf("natasks: ack wait must be positive")
	}

	if c.progressInterval <= 0 {
		return fmt.Errorf("natasks: progress interval must be positive")
	}

	if c.progressInterval >= c.ackWait {
		return fmt.Errorf("natasks: progress interval must be less than ack wait")
	}

	if c.maxAckPending <= 0 {
		return fmt.Errorf("natasks: max ack pending must be positive")
	}

	if c.maxRetries < -1 {
		return fmt.Errorf("natasks: max retries must be greater than or equal to -1")
	}

	if strings.TrimSpace(c.dlqSuffix) == "" {
		return fmt.Errorf("natasks: dlq suffix is required")
	}

	return nil
}

// Option configures a client or worker.
type Option interface {
	applyConfig(*config)
}

// SharedOption can be passed to both client and worker constructors.
type SharedOption interface {
	Option
	WorkerOption
}

type optionFunc func(*config)

func (f optionFunc) applyConfig(cfg *config) {
	f(cfg)
}

func (f optionFunc) applyWorkerConfig(cfg *workerConfig) {
	f(&cfg.config)
}

// WorkerOption configures a worker.
type WorkerOption interface {
	applyWorkerConfig(*workerConfig)
}

type workerOptionFunc func(*workerConfig)

func (f workerOptionFunc) applyWorkerConfig(cfg *workerConfig) {
	f(cfg)
}

// WithStreamName overrides the JetStream stream name.
func WithStreamName(name string) SharedOption {
	return optionFunc(func(cfg *config) {
		cfg.streamName = name
	})
}

// WithSubjectPrefix overrides the publish subject prefix.
func WithSubjectPrefix(prefix string) SharedOption {
	return optionFunc(func(cfg *config) {
		cfg.subjectPrefix = prefix
	})
}

// WithDispatchMiddleware registers middleware for task publication.
func WithDispatchMiddleware(middlewares ...DispatchMiddleware) Option {
	return optionFunc(func(cfg *config) {
		cfg.dispatchMiddleware = append(cfg.dispatchMiddleware, middlewares...)
	})
}

// WithPropagator configures message context propagation for both client and worker.
func WithPropagator(propagator MessagePropagator) SharedOption {
	return optionFunc(func(cfg *config) {
		cfg.propagator = propagator
	})
}

// WithConsumerPrefix overrides the consumer name prefix.
func WithConsumerPrefix(prefix string) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.consumerPrefix = prefix
	})
}

// WithDurable overrides the durable consumer name.
func WithDurable(name string) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.durable = name
	})
}

// WithFetchBatch overrides the worker fetch batch size.
func WithFetchBatch(size int) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.fetchBatch = size
	})
}

// WithConcurrency overrides the number of tasks processed in parallel by the worker.
func WithConcurrency(n int) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.concurrency = n
	})
}

// WithFetchTimeout overrides the worker fetch timeout.
func WithFetchTimeout(timeout time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.fetchTimeout = timeout
	})
}

// WithIdleWait overrides the delay used after an empty poll.
func WithIdleWait(wait time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.idleWait = wait
	})
}

// WithTaskTimeout sets the maximum time allowed for a single handler execution.
// A zero value disables the timeout.
func WithTaskTimeout(timeout time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.taskTimeout = timeout
	})
}

// WithAckWait overrides the consumer AckWait setting.
func WithAckWait(wait time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.ackWait = wait
	})
}

// WithProgressInterval overrides how often the worker sends InProgress while
// a handler is still running.
func WithProgressInterval(interval time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.progressInterval = interval
	})
}

// WithMaxAckPending overrides the consumer MaxAckPending setting.
func WithMaxAckPending(n int) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.maxAckPending = n
	})
}

// WithMaxRetries overrides the maximum number of retries after the first
// failed processing attempt. -1 means unlimited retries.
func WithMaxRetries(n int) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.maxRetries = n
	})
}

// WithRetryBackoff configures retry delays. When the number of retries exceeds
// the provided delays, the last delay is reused.
func WithRetryBackoff(delays ...time.Duration) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.retryBackoff = append([]time.Duration(nil), delays...)
	})
}

// WithDLQSuffix overrides the suffix used for dead-letter queues.
func WithDLQSuffix(suffix string) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.dlqSuffix = suffix
	})
}

// WithProcessMiddleware registers middleware for task processing.
func WithProcessMiddleware(middlewares ...ProcessMiddleware) WorkerOption {
	return workerOptionFunc(func(cfg *workerConfig) {
		cfg.processMiddleware = append(cfg.processMiddleware, middlewares...)
	})
}

func collectConfig(opts []Option) (config, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt.applyConfig(&cfg)
	}

	return cfg, cfg.validate()
}

func collectWorkerConfig(opts []WorkerOption) (workerConfig, error) {
	cfg := defaultWorkerConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt.applyWorkerConfig(&cfg)
	}

	return cfg, cfg.validate()
}
