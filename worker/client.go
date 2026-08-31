package worker

import "context"

// Client is for external producers - no handlers needed.
type Client struct {
    app *App
}

// NewClient creates a producer-only client.
func NewClient(ctx context.Context, opts ...Option) *Client {
    app := New(ctx, opts...)
    return &Client{app: app}
}

// Queue creates a reference to a worker queue.
func (c *Client) Queue(name string) *Execution {
    return &Execution{
        app:      c.app,
        name:     name,
        isClient: true,
    }
}

// Close closes the client's resources.
func (c *Client) Close() error {
    return c.app.Close()
}
