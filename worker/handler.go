package worker

// Handler is ANY function the user wants.
// The framework handles context - users just get their data.
type Handler func(args ...any) error
