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

	app := worker.New(ctx)

	deploy := app.Worker(
		"deploy",
		func(id string) error {
			fmt.Printf("Deploying: %s\n", id)
			time.Sleep(1 * time.Second)
			return nil
		},
		worker.WithConcurrency(10),
		worker.WithTimeout(5*time.Second),
		worker.WithMaxRetries(2),
	)

	health := app.Worker(
		"health",
		func() error {
			fmt.Println("Health check")
			return nil
		},
	)

	cleanup := app.Worker(
		"cleanup",
		func(name string, days int) error {
			fmt.Printf("Cleaning up %s (%d days)\n", name, days)
			return nil
		},
		worker.WithConcurrency(1),
		worker.WithTimeout(60*time.Second),
		worker.WithMaxRetries(1),
	)

	app.Exec(deploy).Args("deployment-123").Submit()
	app.Exec(deploy).Args("deployment-456").Submit()
	app.Exec(health).Submit()
	app.Exec(cleanup).Args("temp-files", 30).Submit()

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app.Shutdown(ctx)
}
