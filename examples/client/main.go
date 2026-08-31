package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Samuel-chibueze/swift-worker/worker"
)

func main() {
	ctx := context.Background()

	client := worker.NewClient(
		ctx,
		worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
	)
	defer client.Close()

	client.Queue("deploy").Args("deployment-123").Submit()
	client.Queue("deploy").Args("api", "v1.2.3", "prod").Submit()
	client.Queue("health").Args("scheduled-check").Submit()

	fmt.Println("All external jobs submitted")

	time.Sleep(2 * time.Second)
}
