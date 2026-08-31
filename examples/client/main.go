package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Samuel-chibueze/swift-worker/worker"
)

func main() {
    ctx := context.Background()

    // NO MORE NewClient - just use worker.New()!
    // This works for external producers too!
    app := worker.New(
        ctx,
        worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
    )

    // Submit jobs using app.Queue() - NO client needed!
    app.Queue("deploy").Args("deployment-123", "v1.0.0", "prod").Submit()
    app.Queue("deploy").Args("api", "v1.2.3", "staging").Submit()
    app.Queue("health").Args("scheduled-check").Submit()

    fmt.Println("✅ All external jobs submitted")

    time.Sleep(2 * time.Second)
}