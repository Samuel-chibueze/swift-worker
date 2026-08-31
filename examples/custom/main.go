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

	// Handler with multiple args
	deploy := app.Worker(
		"deploy",
		func(args ...any) error {
			if len(args) >= 3 {
				service := args[0].(string)
				version := args[1].(string)
				env := args[2].(string)
				fmt.Printf("Deploying %s v%s to %s\n", service, version, env)
				return nil
			}
			fmt.Printf("Deploying with: %v\n", args)
			return nil
		},
		worker.WithConcurrency(10),
		worker.WithTimeout(5*time.Second),
		worker.WithMaxRetries(2),
	)

	health := app.Worker(
		"health",
		func(args ...any) error {
			fmt.Println("Health check")
			return nil
		},
	)

	cleanup := app.Worker(
		"cleanup",
		func(args ...any) error {
			if len(args) >= 2 {
				name := args[0].(string)
				days := args[1].(int)
				fmt.Printf("Cleaning up %s (%d days)\n", name, days)
				return nil
			}
			return nil
		},
		worker.WithConcurrency(1),
		worker.WithTimeout(60*time.Second),
		worker.WithMaxRetries(1),
	)

	app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()
	app.Exec(deploy).Args("auth", "v2.0.0", "staging").Submit()
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
