package tests

import (
	"context"
	"testing"
	"time"

	"github.com/Samuel-chibueze/swift-worker/queue/memory"
	"github.com/Samuel-chibueze/swift-worker/worker"
)

func TestBasicWorker(t *testing.T) {
	ctx := context.Background()
	app := worker.New(ctx)

	executed := false
	w := app.Worker("test", func() error {
		executed = true
		return nil
	})

	backend := memory.New(ctx)
	app.backend = backend

	if err := app.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if err := app.Exec(w).Submit(); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !executed {
		t.Error("Handler was not executed")
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}
