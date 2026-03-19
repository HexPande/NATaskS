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

type SendEmailPayload struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
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

		log.Printf("send email to %s for user %d", payload.Email, payload.UserID)
		return nil
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	log.Print("dispatch messages")
	for i := 0; i < 10; i++ {
		body, err := json.Marshal(SendEmailPayload{
			UserID: 42,
			Email:  "user@example.com",
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
