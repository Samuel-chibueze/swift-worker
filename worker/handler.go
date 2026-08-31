package worker

// Handler is ANY function the user wants.
type Handler func(args ...any) error
