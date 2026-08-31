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
			if len(args) >= 3 {
				fmt.Printf("[%s] deploying %v %v to %v\n",
					time.Now().Format(time.RFC3339),
					args[0], args[1], args[2])
			}
			time.Sleep(2 * time.Second)
			return nil
		},
		worker.WithConcurrency(2),
		worker.WithTimeout(30*time.Second),
	)

	app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}

	select {}
}
