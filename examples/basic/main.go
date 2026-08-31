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
		func(args ...any) error {
			fmt.Printf("Deploying: %v\n", args)
			return nil
		},
		worker.WithConcurrency(4),
	)

	app.Exec(deploy).Args("deployment-123").Submit()
	app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app.Shutdown(ctx)

	fmt.Println("Done!")
}
