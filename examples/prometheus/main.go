package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/hexpande/natasks"
	natprom "github.com/hexpande/natasks/middleware/prometheus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type EmailPayload struct {
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

	registry := prom.NewRegistry()
	metrics, err := natprom.New(natprom.Options{
		Registerer: registry,
		Namespace:  "natasks",
		Subsystem:  "example",
	})
	if err != nil {
		log.Fatal(err)
	}

	client, err := natasks.NewClient(
		js,
		natasks.WithDispatchMiddleware(metrics.DispatchMiddleware()),
	)
	if err != nil {
		log.Fatal(err)
	}

	worker, err := natasks.NewWorker(
		js,
		"emails",
		natasks.WithProcessMiddleware(metrics.ProcessMiddleware("emails")),
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

	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	go func() {
		log.Println("metrics at http://127.0.0.1:8080/metrics")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

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

	if err := client.Dispatch(context.Background(), task, "emails"); err != nil {
		log.Fatal(err)
	}

	select {}
}
