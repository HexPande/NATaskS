package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type InvoicePayload struct {
	InvoiceID string `json:"invoice_id"`
}

func main() {
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	client, err := natasks.NewClient(
		js,
		natasks.WithDispatchMiddleware(func(next natasks.DispatchFunc) natasks.DispatchFunc {
			return func(ctx context.Context, task *natasks.Task, queue string) error {
				task.SetHeader("X-Request-ID", "req-42")
				return next(ctx, task, queue)
			}
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := natasks.NewWorker(
		js,
		"billing",
		natasks.WithProcessMiddleware(func(next natasks.Handler) natasks.Handler {
			return func(ctx context.Context, task *natasks.Task) error {
				log.Printf("middleware saw request id: %s", task.Header("X-Request-ID"))
				return next(ctx, task)
			}
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("billing.charge", func(ctx context.Context, task *natasks.Task) error {
		var payload InvoicePayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		log.Printf("charge invoice %s with request id %s", payload.InvoiceID, task.Header("X-Request-ID"))
		return nil
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	body, err := json.Marshal(InvoicePayload{InvoiceID: "inv-42"})
	if err != nil {
		log.Fatal(err)
	}

	task, err := natasks.NewTask("billing.charge", body)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Dispatch(context.Background(), task, "billing"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second)
}
