package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SendEmailPayload struct {
	User  int    `json:"user"`
	Email string `json:"email"`
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

	client, err := natasks.NewClient(js)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := natasks.NewWorker(
		js,
		"emails",
		natasks.WithConcurrency(4),
		natasks.WithMaxRetries(3),
		natasks.WithRetryBackoff(500*time.Millisecond, time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("emails.send", func(ctx context.Context, task *natasks.Task) error {
		var payload SendEmailPayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		slog.Info("send mail",
			"email", payload.Email,
			"user", payload.User,
		)
		return nil
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	for i := 0; i < 1000; i++ {
		body, err := json.Marshal(SendEmailPayload{
			User:  i,
			Email: fmt.Sprintf("user%d@example.com", i),
		})
		if err != nil {
			log.Fatal(err)
		}

		task, err := natasks.NewTask("emails.send", body)
		if err != nil {
			log.Fatal(err)
		}

		if err := client.Dispatch(context.Background(), task, "emails"); err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(5 * time.Second)
}
