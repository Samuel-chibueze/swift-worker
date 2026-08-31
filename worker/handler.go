package worker

import (
    "fmt"
    "reflect"
)

// Handler is the internal wrapped function type
type Handler func(args ...any) error

// WrapHandler wraps ANY function into a Handler using reflection
func WrapHandler(fn interface{}) Handler {
    return func(args ...any) error {
        v := reflect.ValueOf(fn)
        
        // Check if it's a function
        if v.Kind() != reflect.Func {
            return fmt.Errorf("handler is not a function (got %T)", fn)
        }

        // Prepare arguments for the function call
        in := make([]reflect.Value, len(args))
        for i, arg := range args {
            in[i] = reflect.ValueOf(arg)
        }

        // Call the function
        result := v.Call(in)
        
        // Handle return values
        if len(result) == 0 {
            return nil
        }
        
        // Check if last return is error
        if err, ok := result[len(result)-1].Interface().(error); ok {
            return err
        }
        
        return nil
    }
}
