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

type ReminderPayload struct {
	Message string `json:"message"`
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

	worker, err := natasks.NewWorker(js, "reminders")
	if err != nil {
		log.Fatal(err)
	}

	worker.Handle("reminders.send", func(ctx context.Context, task *natasks.Task) error {
		var payload ReminderPayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		log.Printf("reminder delivered: %s", payload.Message)
		return nil
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	body, err := json.Marshal(ReminderPayload{Message: "drink water"})
	if err != nil {
		log.Fatal(err)
	}

	task, err := natasks.NewTask("reminders.send", body)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.DispatchIn(context.Background(), task, "reminders", 2*time.Second); err != nil {
		log.Fatal(err)
	}

	at := time.Now().Add(4 * time.Second)
	if err := client.DispatchAt(context.Background(), task, "reminders", at); err != nil {
		log.Fatal(err)
	}

	time.Sleep(6 * time.Second)
}
