# Examples

Each example is a small standalone program that shows one feature set in isolation.

Examples:

- `basic`: immediate dispatch and worker processing
- `delayed`: delayed and scheduled delivery with `DispatchIn` and `DispatchAt`
- `retries-dlq`: retries, backoff, `NoRetry`, and dead-letter queues
- `middleware-headers`: dispatch/process middleware and task headers
- `otel`: OpenTelemetry spans and trace-context propagation
- `prometheus`: Prometheus middleware setup and custom registry usage

Run any example with:

```bash
go run ./examples/basic
```

All examples expect a local NATS server with JetStream enabled at `nats://127.0.0.1:4222`.
