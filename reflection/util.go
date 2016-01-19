package reflection

import (
	"reflect"
)

// Setter function.
type Setter func(target interface{}, value interface{})

// Getter function.
type Getter func(target interface{}) interface{}

// NoopSetter is setter without effect.
func NoopSetter(target interface{}, value interface{}) {}

// MakeSetter makes setter function for given field.
func MakeSetter(f reflect.StructField) Setter {
	return func(target interface{}, value interface{}) {
		var r = reflect.ValueOf(target)
		if r.Kind() == reflect.Ptr {
			r = r.Elem()
		}
		var v = reflect.ValueOf(value)
		r.FieldByName(f.Name).Set(v)
	}
}

// MakeGetter makes getter function for given field.
func MakeGetter(f reflect.StructField) Getter {
	return func(target interface{}) interface{} {
		return GetValue(target, f)
	}
}

// GetValue of given field.
func GetValue(target interface{}, f reflect.StructField) interface{} {
	var r = reflect.ValueOf(target)
	if r.Kind() == reflect.Ptr {
		r = r.Elem()
	}
	return r.FieldByName(f.Name).Interface()
}
