package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/Samuel-chibueze/swift-worker/worker"
)

func handleDeploy(service, version, env string) error {
    fmt.Printf("?? Deploying %s version %s to %s\n", service, version, env)
    return nil
}

func handleCleanup(name string) error {
    fmt.Printf("?? Cleaning up: %s\n", name)
    return nil
}

func handleHealth() error {
    fmt.Println("?? Health check")
    return nil
}

type DeployJob struct {
    Service string
    Version string
    Env     string
}

func handleStruct(job DeployJob) error {
    fmt.Printf("?? Deploying struct: %+v\n", job)
    return nil
}

func main() {
    ctx := context.Background()

    fmt.Println("?? Testing with ANY function signatures...")
    app := worker.New(ctx)

    deploy := app.Worker("deploy", handleDeploy, worker.WithConcurrency(2))
    cleanup := app.Worker("cleanup", handleCleanup)
    health := app.Worker("health", handleHealth)
    structDeploy := app.Worker("struct", handleStruct)

    app.Exec(deploy).Args("api", "v1.2.3", "prod").Submit()
    app.Exec(deploy).Args("auth", "v2.0.0", "staging").Submit()
    app.Exec(cleanup).Args("temp-files").Submit()
    app.Exec(health).Submit()
    app.Exec(structDeploy).Args(DeployJob{
        Service: "gateway",
        Version: "v3.0.0",
        Env:     "prod",
    }).Submit()

    if err := app.Start(); err != nil {
        log.Fatal(err)
    }

    time.Sleep(2 * time.Second)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    app.Shutdown(ctx)

    fmt.Println("? All tests passed!")
}
