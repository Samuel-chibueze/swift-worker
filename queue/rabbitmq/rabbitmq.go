package rabbitmq

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"

    amqp "github.com/rabbitmq/amqp091-go"

    "github.com/Samuel-chibueze/swift-worker/types"
)

type Backend struct {
    mu        sync.Mutex
    conn      *amqp.Connection
    ch        *amqp.Channel
    url       string
    queueName string
    ctx       context.Context
    cancel    context.CancelFunc
    wg        sync.WaitGroup
    started   bool
}

func New(ctx context.Context, url string) (*Backend, error) {
    if url == "" {
        return nil, fmt.Errorf("rabbitmq URL is empty")
    }

    ctx, cancel := context.WithCancel(ctx)

    b := &Backend{
        url:       url,
        queueName: "swift-worker",
        ctx:       ctx,
        cancel:    cancel,
    }

    if err := b.connect(); err != nil {
        cancel()
        return nil, err
    }

    fmt.Printf("[RabbitMQ] ? Connected to %s\n", url)
    return b, nil
}

func (b *Backend) connect() error {
    b.mu.Lock()
    defer b.mu.Unlock()

    fmt.Printf("[RabbitMQ] Connecting to %s...\n", b.url)

    conn, err := amqp.Dial(b.url)
    if err != nil {
        return fmt.Errorf("dial rabbitmq: %w", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return fmt.Errorf("create channel: %w", err)
    }

    q, err := ch.QueueDeclare(
        b.queueName,
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        ch.Close()
        conn.Close()
        return fmt.Errorf("declare queue: %w", err)
    }

    fmt.Printf("[RabbitMQ] Queue declared: %s (messages: %d)\n", q.Name, q.Messages)

    err = ch.Qos(10, 0, false)
    if err != nil {
        ch.Close()
        conn.Close()
        return fmt.Errorf("set qos: %w", err)
    }

    b.conn = conn
    b.ch = ch
    b.started = true

    fmt.Printf("[RabbitMQ] ? Ready, queue: %s\n", b.queueName)
    return nil
}

func (b *Backend) Enqueue(ctx context.Context, job types.Job) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.ch == nil {
        return fmt.Errorf("channel is closed")
    }

    // Marshal the ENTIRE job to JSON
    body, err := json.Marshal(job)
    if err != nil {
        return fmt.Errorf("marshal job: %w", err)
    }

    err = b.ch.PublishWithContext(
        ctx,
        "",
        b.queueName,
        false,
        false,
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            Body:         body,
        },
    )

    if err != nil {
        return fmt.Errorf("publish job: %w", err)
    }

    fmt.Printf("[RabbitMQ] ?? Published job: %s\n", job.ID)
    return nil
}

func (b *Backend) Start(ctx context.Context, jobs chan<- types.Job) error {
    b.mu.Lock()
    if b.ch == nil {
        b.mu.Unlock()
        return fmt.Errorf("channel is nil")
    }
    b.mu.Unlock()

    fmt.Printf("[RabbitMQ] ?? Starting consumer on queue: %s\n", b.queueName)

    deliveries, err := b.ch.Consume(
        b.queueName,
        "swift-consumer",
        false,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        return fmt.Errorf("consume: %w", err)
    }

    b.wg.Add(1)
    go b.consumeLoop(ctx, deliveries, jobs)

    fmt.Printf("[RabbitMQ] ? Consumer started, waiting for messages...\n")
    return nil
}

func (b *Backend) consumeLoop(ctx context.Context, deliveries <-chan amqp.Delivery, jobs chan<- types.Job) {
    defer b.wg.Done()

    fmt.Printf("[RabbitMQ] ?? Consume loop running\n")

    for {
        select {
        case <-ctx.Done():
            fmt.Printf("[RabbitMQ] ? Consume loop stopped (context cancelled)\n")
            return

        case delivery, ok := <-deliveries:
            if !ok {
                fmt.Printf("[RabbitMQ] ? Delivery channel closed\n")
                return
            }

            fmt.Printf("[RabbitMQ] ?? Received message (%d bytes)\n", len(delivery.Body))

            var job types.Job
            if err := json.Unmarshal(delivery.Body, &job); err != nil {
                fmt.Printf("[RabbitMQ] ? Failed to unmarshal: %v\n", err)
                _ = delivery.Nack(false, false)
                continue
            }

            fmt.Printf("[RabbitMQ] ?? Forwarding job: %s (worker: %s)\n", job.ID, job.Worker)

            select {
            case jobs <- job:
                fmt.Printf("[RabbitMQ] ? Job %s forwarded\n", job.ID)
                if err := delivery.Ack(false); err != nil {
                    fmt.Printf("[RabbitMQ] ? Failed to ACK: %v\n", err)
                }
            case <-ctx.Done():
                fmt.Printf("[RabbitMQ] ? Context cancelled, not forwarding\n")
                _ = delivery.Nack(false, true)
                return
            }
        }
    }
}

func (b *Backend) Close() error {
    b.mu.Lock()
    defer b.mu.Unlock()

    fmt.Printf("[RabbitMQ] ?? Closing...\n")

    if b.cancel != nil {
        b.cancel()
    }
    b.wg.Wait()

    var errs []error

    if b.ch != nil {
        if err := b.ch.Close(); err != nil {
            errs = append(errs, fmt.Errorf("close channel: %w", err))
        }
    }

    if b.conn != nil {
        if err := b.conn.Close(); err != nil {
            errs = append(errs, fmt.Errorf("close connection: %w", err))
        }
    }

    if len(errs) > 0 {
        return fmt.Errorf("close errors: %v", errs)
    }

    fmt.Printf("[RabbitMQ] ? Closed\n")
    return nil
}
