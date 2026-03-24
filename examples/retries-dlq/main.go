package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/hexpande/natasks"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ReportPayload struct {
	Kind string `json:"kind"`
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
		"reports",
		natasks.WithMaxRetries(2),
		natasks.WithRetryBackoff(time.Second, 2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	var attempts atomic.Int32
	worker.Handle("reports.generate", func(ctx context.Context, task *natasks.Task) error {
		var payload ReportPayload
		if err := task.Unmarshal(&payload); err != nil {
			return natasks.NoRetry(err)
		}

		switch payload.Kind {
		case "validation":
			return natasks.NoRetry(errors.New("invalid report request"))
		case "transient":
			if attempts.Add(1) < 3 {
				return errors.New("temporary upstream error")
			}
			log.Println("report generated after retry")
			return nil
		case "permanent":
			return errors.New("report source unavailable")
		default:
			return nil
		}
	})

	go func() {
		if err := worker.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	for _, kind := range []string{"validation", "transient", "permanent"} {
		body, err := json.Marshal(ReportPayload{Kind: kind})
		if err != nil {
			log.Fatal(err)
		}

		task, err := natasks.NewTask("reports.generate", body)
		if err != nil {
			log.Fatal(err)
		}

		if err := client.Dispatch(context.Background(), task, "reports"); err != nil {
			log.Fatal(err)
		}
	}

	log.Println("check the reports-dlq queue in NATS for permanently failed tasks")
	time.Sleep(8 * time.Second)
}
