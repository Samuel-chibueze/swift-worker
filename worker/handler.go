package worker

import (
    "fmt"
    "reflect"
)

// Handler is ANY function - we use reflection to call it
type Handler func(args ...any) error

// WrapHandler wraps ANY function into a Handler
func WrapHandler(fn interface{}) Handler {
    return func(args ...any) error {
        v := reflect.ValueOf(fn)
        if v.Kind() != reflect.Func {
            return fmt.Errorf("handler is not a function")
        }

        // Prepare arguments
        in := make([]reflect.Value, len(args))
        for i, arg := range args {
            in[i] = reflect.ValueOf(arg)
        }

        // Call the function
        result := v.Call(in)
        
        // Check return values
        if len(result) == 0 {
            return nil
        }
        
        // If last return is error
        if err, ok := result[len(result)-1].Interface().(error); ok {
            return err
        }
        
        return nil
    }
}
