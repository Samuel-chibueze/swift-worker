package worker

import (
    "context"
    "fmt"
    "reflect"
)

// Handler is the internal wrapped function type
// It receives a context for timeout/cancellation control
type Handler func(ctx context.Context, args ...any) error

// WrapHandler wraps ANY function into a Handler using reflection
func WrapHandler(fn interface{}) Handler {
    return func(ctx context.Context, args ...any) error {
        v := reflect.ValueOf(fn)
        
        if v.Kind() != reflect.Func {
            return fmt.Errorf("handler is not a function (got %T)", fn)
        }

        funcType := v.Type()
        
        // Check if function expects context
        hasContext := false
        startIdx := 0
        if funcType.NumIn() > 0 && funcType.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem() {
            hasContext = true
            startIdx = 1
        }
        
        // Check number of arguments
        expectedArgs := funcType.NumIn() - startIdx
        if expectedArgs != len(args) {
            return fmt.Errorf("expected %d arguments, got %d", expectedArgs, len(args))
        }

        // Prepare arguments for the function call
        in := make([]reflect.Value, 0, funcType.NumIn())
        
        // Add context if function expects it
        if hasContext {
            in = append(in, reflect.ValueOf(ctx))
        }
        
        // Add the rest of the arguments
        for i := 0; i < len(args); i++ {
            in = append(in, reflect.ValueOf(args[i]))
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
