package worker

// Handler is ANY function the user wants.
// The user decides what parameters to take.
// The framework handles everything else.
type Handler func(args ...any) error
