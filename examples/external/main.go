package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

// This is an EXTERNAL producer that doesn't register any workers.
// It just submits jobs using app.Queue() - NO client needed!
func main() {
	ctx := context.Background()

	fmt.Println("========================================")
	fmt.Println("?? EXTERNAL PRODUCER")
	fmt.Println("========================================")
	fmt.Println()

	// Create app with RabbitMQ - NO workers registered!
	app := worker.New(
		ctx,
		worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
	)

	fmt.Println("? App created with RabbitMQ backend")
	fmt.Println("?? Submitting jobs externally...")
	fmt.Println()

	// Submit jobs using app.Queue() - NO Exec() needed!
	jobs := []struct {
		name string
		args []any
	}{
		{"deploy", []any{"api-service", "v1.2.3", "production"}},
		{"deploy", []any{"auth-service", "v2.0.0", "staging"}},
		{"deploy", []any{"gateway-service", "v3.0.0", "prod"}},
		{"cleanup", []any{"temp-files"}},
		{"health", []any{}},
	}

	for i, job := range jobs {
		// Direct Queue submission - no Exec() wrapper!
		err := app.Queue(job.name).Args(job.args...).Submit()
		if err != nil {
			log.Printf("? Job %d error: %v", i+1, err)
		} else {
			fmt.Printf("? Job %d submitted: %s %v\n", i+1, job.name, job.args)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("? All jobs submitted!")
	fmt.Println("")
	fmt.Println("?? These jobs will be processed by ANY worker process")
	fmt.Println("   that has the corresponding workers registered.")
}
