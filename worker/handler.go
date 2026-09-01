package worker

import (
    "context"
    "fmt"
    "reflect"
)

type Handler func(ctx context.Context, args ...any) error

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

        // Prepare arguments
        in := make([]reflect.Value, 0, funcType.NumIn())

        if hasContext {
            in = append(in, reflect.ValueOf(ctx))
        }

        // Convert args to the expected types
        for i := 0; i < len(args); i++ {
            arg := args[i]
            argType := funcType.In(startIdx + i)

            // If the argument is nil and the type is not interface{}, create a zero value
            if arg == nil {
                in = append(in, reflect.Zero(argType))
                continue
            }

            // Try to convert if types don't match
            val := reflect.ValueOf(arg)
            if !val.Type().AssignableTo(argType) {
                // Try to convert
                if val.Type().ConvertibleTo(argType) {
                    val = val.Convert(argType)
                } else {
                    return fmt.Errorf("argument %d: expected %s, got %s", i, argType, val.Type())
                }
            }
            in = append(in, val)
        }

        // Call the function
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
