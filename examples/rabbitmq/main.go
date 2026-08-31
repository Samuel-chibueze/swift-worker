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

	deploy := app.Worker(
		"deploy",
		func(args ...any) error {
			fmt.Printf("[%s] Deploying: %v\n", time.Now().Format(time.RFC3339), args)
			time.Sleep(2 * time.Second)
			return nil
		},
		worker.WithConcurrency(4),
		worker.WithTimeout(10*time.Second),
		worker.WithMaxRetries(5),
	)

	app.Exec(deploy).Args("deployment-123").Submit()
	app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app.Shutdown(ctx)
}
