package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

// Handler is the internal handler function signature used by the framework.
type Handler func(ctx context.Context, args ...any) error

// WrapHandler wraps a user-provided function into the framework Handler.
// It supports functions that optionally accept context.Context as the first parameter,
// followed by typed arguments. JSON-compatible maps/slices will be converted into the
// expected parameter types via a JSON round-trip when necessary.
func WrapHandler(fn interface{}) Handler {
	return func(ctx context.Context, args ...any) error {
		v := reflect.ValueOf(fn)
		if v.Kind() != reflect.Func {
			return fmt.Errorf("handler is not a function (got %T)", fn)
		}

		funcType := v.Type()

		// Determine if the function expects context.Context as first parameter.
		hasContext := false
		startIdx := 0
		if funcType.NumIn() > 0 && funcType.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem() {
			hasContext = true
			startIdx = 1
		}

		expectedArgs := funcType.NumIn() - startIdx
		if expectedArgs != len(args) {
			return fmt.Errorf("expected %d arguments, got %d", expectedArgs, len(args))
		}

		// Build the reflect.Value slice for calling the function.
		in := make([]reflect.Value, 0, funcType.NumIn())
		if hasContext {
			in = append(in, reflect.ValueOf(ctx))
		}

		for i := 0; i < len(args); i++ {
			arg := args[i]
			paramType := funcType.In(startIdx + i)

			// Nil handling
			if arg == nil {
				in = append(in, reflect.Zero(paramType))
				continue
			}

			val := reflect.ValueOf(arg)

			// If argument is already assignable to the parameter type, use it directly.
			if val.Type().AssignableTo(paramType) {
				in = append(in, val)
				continue
			}

			// If the parameter type is an interface, accept the value (no conversion).
			if paramType.Kind() == reflect.Interface {
				in = append(in, val)
				continue
			}

			// Attempt JSON round-trip conversion:
			// Marshal the provided arg to JSON, then unmarshal into the expected parameter type.
			// This handles map[string]interface{} -> struct conversion, []any -> []T, etc.
			b, err := json.Marshal(arg)
			if err != nil {
				return fmt.Errorf("argument %d: failed to marshal for conversion: %w", i, err)
			}

			// Create a new value of the param type (pointer), unmarshal into it, then extract the element.
			dstPtr := reflect.New(paramType)
			if err := json.Unmarshal(b, dstPtr.Interface()); err != nil {
				return fmt.Errorf("argument %d: failed to unmarshal into %s: %w", i, paramType, err)
			}
			in = append(in, dstPtr.Elem())
		}

		// Call the function via reflection.
		result := v.Call(in)

		// If function returns an error as its last return value, return it.
		if len(result) == 0 {
			return nil
		}

		if errVal, ok := result[len(result)-1].Interface().(error); ok {
			return errVal
		}
		return nil
	}
}
