package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

func main() {
	ctx := context.Background()

	app := worker.New(
		ctx,
		worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
	)

	cleanup := app.Worker(
		"cleanup",
		func(args ...any) error {
			fmt.Printf("[%s] cleaning up: %v\n", time.Now().Format(time.RFC3339), args)
			return nil
		},
		worker.WithConcurrency(1),
	)

	app.Schedule("*/30 * * * * *", func() {
		app.Exec(cleanup).Args("scheduled-cleanup").Submit()
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Scheduler started")
	select {}
}
