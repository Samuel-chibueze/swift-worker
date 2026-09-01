package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/google/uuid"
)

// ? Use json.RawMessage for Args
type Job struct {
    ID        string          `json:"id"`
    Worker    string          `json:"worker"`
    Args      json.RawMessage `json:"args"`  // ? Raw JSON, not string!
    CreatedAt time.Time       `json:"created_at"`
}

func main() {
    ctx := context.Background()

    fmt.Println("========================================")
    fmt.Println("?? FINAL PUBLISHER - CORRECT FORMAT")
    fmt.Println("========================================")
    fmt.Println()

    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatalf("? Failed to connect: %v", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("? Failed to create channel: %v", err)
    }
    defer ch.Close()

    queueName := "swift-worker"
    _, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
    if err != nil {
        log.Fatalf("? Failed to declare queue: %v", err)
    }

    fmt.Printf("? Queue declared: %s\n", queueName)
    fmt.Println()

    // ============================================================
    // Multiple args - send as JSON array
    // ============================================================
    args1 := []string{"api-service", "v1.2.3", "production"}
    argsJSON1, _ := json.Marshal(args1)

    job1 := Job{
        ID:        uuid.New().String(),
        Worker:    "deploy",
        Args:      json.RawMessage(argsJSON1), // ? Raw JSON array
        CreatedAt: time.Now().UTC(),
    }
    publishJob(ctx, ch, queueName, job1)

    // ============================================================
    // Single string - send as JSON string
    // ============================================================
    args2 := "hello-world"
    argsJSON2, _ := json.Marshal(args2)

    job2 := Job{
        ID:        uuid.New().String(),
        Worker:    "deploy",
        Args:      json.RawMessage(argsJSON2), // ? Raw JSON string
        CreatedAt: time.Now().UTC(),
    }
    publishJob(ctx, ch, queueName, job2)

    // ============================================================
    // Map - send as JSON object
    // ============================================================
    args3 := map[string]interface{}{
        "service": "auth",
        "version": "v2.0.0",
        "env":     "staging",
    }
    argsJSON3, _ := json.Marshal(args3)

    job3 := Job{
        ID:        uuid.New().String(),
        Worker:    "deploy",
        Args:      json.RawMessage(argsJSON3), // ? Raw JSON object
        CreatedAt: time.Now().UTC(),
    }
    publishJob(ctx, ch, queueName, job3)

    fmt.Println()
    fmt.Println("? All jobs published successfully!")
    fmt.Println()
    fmt.Println("?? Check the worker - handlers should now execute!")
}

func publishJob(ctx context.Context, ch *amqp.Channel, queueName string, job Job) {
    jobJSON, err := json.Marshal(job)
    if err != nil {
        log.Printf("? Failed to marshal job: %v", err)
        return
    }

    err = ch.PublishWithContext(
        ctx,
        "",
        queueName,
        false,
        false,
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            Body:         jobJSON,
        },
    )
    if err != nil {
        log.Printf("? Failed to publish: %v", err)
        return
    }

    fmt.Printf("? Published: ID=%s, Worker=%s, Args=%s\n",
        job.ID, job.Worker, string(job.Args))
}
