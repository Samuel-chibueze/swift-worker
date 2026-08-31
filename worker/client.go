package worker

import "context"

type Client struct {
	app *App
}

func NewClient(ctx context.Context, opts ...Option) *Client {
	app := New(ctx, opts...)
	return &Client{app: app}
}

func (c *Client) Queue(name string) *Execution {
	return &Execution{
		app:      c.app,
		name:     name,
		isClient: true,
	}
}

func (c *Client) Close() error {
	return c.app.Close()
}
