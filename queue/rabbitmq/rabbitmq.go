package rabbitmq

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

// Job is defined locally - no import from worker
type Job struct {
    ID        string          `json:"id"`
    Worker    string          `json:"worker"`
    Args      json.RawMessage `json:"args"`
    CreatedAt time.Time       `json:"created_at"`
}

type Backend struct {
    mu        sync.Mutex
    conn      *amqp.Connection
    ch        *amqp.Channel
    url       string
    queueName string
    ctx       context.Context
    cancel    context.CancelFunc
    wg        sync.WaitGroup
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

    return b, nil
}

func (b *Backend) connect() error {
    b.mu.Lock()
    defer b.mu.Unlock()

    conn, err := amqp.Dial(b.url)
    if err != nil {
        return fmt.Errorf("dial rabbitmq: %w", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return fmt.Errorf("create channel: %w", err)
    }

    _, err = ch.QueueDeclare(
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

    err = ch.Qos(10, 0, false)
    if err != nil {
        ch.Close()
        conn.Close()
        return fmt.Errorf("set qos: %w", err)
    }

    b.conn = conn
    b.ch = ch
    return nil
}

func (b *Backend) Enqueue(ctx context.Context, job interface{}) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.ch == nil {
        return fmt.Errorf("channel is closed")
    }

    var j Job
    switch v := job.(type) {
    case map[string]interface{}:
        data, err := json.Marshal(v)
        if err != nil {
            return fmt.Errorf("marshal job: %w", err)
        }
        if err := json.Unmarshal(data, &j); err != nil {
            return fmt.Errorf("unmarshal job: %w", err)
        }
    case Job:
        j = v
    case *Job:
        j = *v
    default:
        data, err := json.Marshal(job)
        if err != nil {
            return fmt.Errorf("marshal job: %w", err)
        }
        if err := json.Unmarshal(data, &j); err != nil {
            return fmt.Errorf("unmarshal job: %w", err)
        }
    }

    body, err := json.Marshal(j)
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

    return nil
}

func (b *Backend) Start(ctx context.Context, jobs chan<- interface{}) error {
    b.mu.Lock()
    if b.ch == nil {
        b.mu.Unlock()
        return fmt.Errorf("channel is nil")
    }
    b.mu.Unlock()

    deliveries, err := b.ch.Consume(
        b.queueName,
        "",
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

    return nil
}

func (b *Backend) consumeLoop(ctx context.Context, deliveries <-chan amqp.Delivery, jobs chan<- interface{}) {
    defer b.wg.Done()

    for {
        select {
        case <-ctx.Done():
            return

        case delivery, ok := <-deliveries:
            if !ok {
                return
            }

            var job Job
            if err := json.Unmarshal(delivery.Body, &job); err != nil {
                _ = delivery.Nack(false, false)
                continue
            }

            select {
            case jobs <- job:
                _ = delivery.Ack(false)
            case <-ctx.Done():
                _ = delivery.Nack(false, true)
                return
            }
        }
    }
}

func (b *Backend) Close() error {
    b.mu.Lock()
    defer b.mu.Unlock()

    b.cancel()
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

    return nil
}
