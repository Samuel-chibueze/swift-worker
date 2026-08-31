package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/Samuel-chibueze/swift-worker/worker"
)

func handleDeploy(service, version, env string) error {
    fmt.Printf("[%s] ?? deploying %s v%s to %s\n",
        time.Now().Format(time.RFC3339),
        service, version, env)
    time.Sleep(2 * time.Second)
    return nil
}

func main() {
    ctx := context.Background()

    app := worker.New(
        ctx,
        worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
    )

    deploy := app.Worker("deploy", handleDeploy, 
        worker.WithConcurrency(2),
        worker.WithTimeout(30*time.Second),
    )

    app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()

    if err := app.Start(); err != nil {
        log.Fatal(err)
    }

    select {}
}
