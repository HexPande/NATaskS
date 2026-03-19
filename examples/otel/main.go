package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hexpande/natasks"
	natotel "github.com/hexpande/natasks/middleware/otel"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	gootel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type EmailPayload struct {
	Email string `json:"email"`
}

func main() {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	gootel.SetTracerProvider(tp)
	gootel.SetTextMapPropagator(propagation.TraceContext{})

	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	otelMiddleware := natotel.New(natotel.Options{
		TracerProvider: tp,
		Propagator:     propagation.TraceContext{},
	})

	client, err := natasks.NewClient(
		js,
		natasks.WithPropagator(otelMiddleware),
		natasks.WithDispatchMiddleware(otelMiddleware.DispatchMiddleware()),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := natasks.NewWorker(
		js,
		"emails",
		natasks.WithPropagator(otelMiddleware),
		natasks.WithProcessMiddleware(otelMiddleware.ProcessMiddleware("emails")),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("emails.send", func(ctx context.Context, task *natasks.Task) error {
		var payload EmailPayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		log.Printf("send email to %s", payload.Email)
		return nil
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	body, err := json.Marshal(EmailPayload{Email: "user@example.com"})
	if err != nil {
		log.Fatal(err)
	}

	task, err := natasks.NewTask("emails.send", body)
	if err != nil {
		log.Fatal(err)
	}

	ctx, span := tp.Tracer("example").Start(context.Background(), "send-welcome-email")
	defer span.End()

	if err := client.Dispatch(ctx, task, "emails"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}
