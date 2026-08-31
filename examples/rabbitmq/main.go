package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/Samuel-chibueze/swift-worker/worker"
)

func handleDeploy(service, version, env string) error {
    fmt.Printf("[%s] ?? Deploying %s version %s to %s\n", 
        time.Now().Format(time.RFC3339),
        service, version, env)
    time.Sleep(1 * time.Second)
    return nil
}

func main() {
    ctx := context.Background()

    app := worker.New(
        ctx,
        worker.WithRabbitMQ("amqp://guest:guest@localhost:5672/"),
    )

    deploy := app.Worker("deploy", handleDeploy, 
        worker.WithConcurrency(4),
        worker.WithTimeout(10*time.Second),
        worker.WithMaxRetries(3),
    )

    for i := 1; i <= 5; i++ {
        app.Exec(deploy).Args(
            fmt.Sprintf("service-%d", i),
            fmt.Sprintf("v1.0.%d", i),
            "prod",
        ).Submit()
    }

    fmt.Println("?? All jobs submitted")

    if err := app.Start(); err != nil {
        log.Fatal(err)
    }

    time.Sleep(10 * time.Second)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    app.Shutdown(ctx)

    fmt.Println("? Done!")
}
