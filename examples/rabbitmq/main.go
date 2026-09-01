package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	fmt.Println("========================================")
	fmt.Println("?? SWIFT WORKER WITH RABBITMQ")
	fmt.Println("========================================")
	fmt.Println()

	// Create app with RabbitMQ
	app := worker.New(
		ctx,
		worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
		worker.WithDefaultTimeout(30*time.Second),
		worker.WithDefaultRetries(3),
		worker.WithDefaultConcurrency(2),
	)

	// Register workers
	app.Worker(
		"deploy",
		func(service, version, env string) error {
			fmt.Printf("[%s] ?? Deploying %s version %s to %s\n",
				time.Now().Format(time.RFC3339),
				service, version, env)
			time.Sleep(1 * time.Second)
			return nil
		},
		worker.WithConcurrency(4),
	)

	app.Worker(
		"cleanup",
		func(name string) error {
			fmt.Printf("[%s] ?? Cleaning up: %s\n",
				time.Now().Format(time.RFC3339), name)
			return nil
		},
		worker.WithConcurrency(1),
	)

	app.Worker(
		"health",
		func() error {
			fmt.Printf("[%s] ?? Health check\n",
				time.Now().Format(time.RFC3339))
			return nil
		},
		worker.WithConcurrency(5),
	)

	fmt.Println("? Workers registered: deploy, cleanup, health")
	fmt.Println("")

	// Submit jobs using app.Queue() - NO Exec() needed!
	fmt.Println("?? Submitting internal jobs...")

	// Direct Queue submission - no Exec() wrapper!
	app.Queue("deploy").Args("api-service", "v1.2.3", "production").Submit()
	app.Queue("deploy").Args("auth-service", "v2.0.0", "staging").Submit()
	app.Queue("cleanup").Args("temp-files").Submit()
	app.Queue("health").Submit()

	fmt.Println("?? Jobs submitted. Worker running...")
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	if err := app.Run(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("?? Worker stopped")
}
