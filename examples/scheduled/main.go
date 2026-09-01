package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

func handleCleanup(name string) error {
	fmt.Printf("[%s] ?? cleaning up: %s\n", time.Now().Format(time.RFC3339), name)
	return nil
}

func main() {
	ctx := context.Background()

	app := worker.New(
		ctx,
		worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
	)

	cleanup := app.Worker("cleanup", handleCleanup, worker.WithConcurrency(1))

	app.Schedule("*/30 * * * * *", func() {
		app.Exec(cleanup).Args("scheduled-cleanup").Submit()
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("? Scheduler started")
	select {}
}
