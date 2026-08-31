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
        
        if v.Kind() != reflect.Func {
            return fmt.Errorf("handler is not a function (got %T)", fn)
        }

        funcType := v.Type()
        if funcType.NumIn() != len(args) {
            return fmt.Errorf("expected %d arguments, got %d", funcType.NumIn(), len(args))
        }

        in := make([]reflect.Value, len(args))
        for i, arg := range args {
            in[i] = reflect.ValueOf(arg)
        }

        result := v.Call(in)
        
        if len(result) == 0 {
            return nil
        }
        
        if err, ok := result[len(result)-1].Interface().(error); ok {
            return err
        }
        
        return nil
    }
}
