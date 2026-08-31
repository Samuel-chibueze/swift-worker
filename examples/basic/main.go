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

    fmt.Println("========================================")
    fmt.Println("?? BASIC SWIFT WORKER (Memory Backend)")
    fmt.Println("========================================")
    fmt.Println()

    // Memory backend (default) - NO RabbitMQ needed!
    app := worker.New(
        ctx,
        worker.WithDefaultTimeout(30*time.Second),
        worker.WithDefaultRetries(3),
        worker.WithDefaultConcurrency(2),
    )

    deploy := app.Worker(
        "deploy",
        func(service, version, env string) error {
            fmt.Printf("[%s] ?? Deploying %s version %s to %s\n",
                time.Now().Format(time.RFC3339),
                service, version, env)
            return nil
        },
        worker.WithConcurrency(4),
    )

    // Use Exec() for registered workers
    fmt.Println("?? Submitting job...")
    err := app.Exec(deploy).Args("api-service", "v1.2.3", "production").Submit()
    if err != nil {
        log.Fatalf("? Submit error: %v", err)
    }

    fmt.Println("??  Starting worker...")
    if err := app.Start(); err != nil {
        log.Fatalf("? Start failed: %v", err)
    }

    time.Sleep(2 * time.Second)

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := app.Shutdown(shutdownCtx); err != nil {
        log.Printf("? Shutdown error: %v", err)
    }

    fmt.Println()
    fmt.Println("? Test complete!")
}
