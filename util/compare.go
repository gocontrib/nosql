package util

import (
	"strings"
)

// Built-in types
// bool
// string
// int  int8  int16  int32  int64
// uint uint8 uint16 uint32 uint64 uintptr
// byte // alias for uint8
// rune // alias for int32 ~= a character (Unicode code point)
// float32 float64
// complex64 complex128

// Equals determines equality of two interface values.
func Equals(a, b interface{}) bool {
	return Compare(a, b) == 0
}

// Compare of two interface values.
func Compare(x, y interface{}) int {
	if x == nil {
		if y == nil {
			return 0
		}
		return -1
	}
	if y == nil {
		return 1
	}
	switch a := x.(type) {
	case bool:
		switch b := y.(type) {
		case bool:
			return intcmp(boolToInt64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(boolToInt64(a), int64(b))
		case uint:
			return intcmp(boolToInt64(a), int64(b))
		case int8:
			return intcmp(boolToInt64(a), int64(b))
		case uint8:
			return intcmp(boolToInt64(a), int64(b))
		case int16:
			return intcmp(boolToInt64(a), int64(b))
		case uint16:
			return intcmp(boolToInt64(a), int64(b))
		case int32:
			return intcmp(boolToInt64(a), int64(b))
		case uint32:
			return intcmp(boolToInt64(a), int64(b))
		case int64:
			return intcmp(boolToInt64(a), b)
		case uint64:
			return uintcmp(uint64(boolToInt64(a)), b)
		case float32:
			return floatcmp(boolToFloat64(a), float64(b))
		case float64:
			return floatcmp(boolToFloat64(a), b)
		}
	case string:
		switch b := y.(type) {
		case string:
			return strings.Compare(a, b)
		default:
			return 1
		}
	case int:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case int8:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case uint8:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case int16:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case uint16:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case int32:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case uint32:
		switch b := y.(type) {
		case bool:
			return intcmp(int64(a), boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(int64(a), int64(b))
		case uint:
			return intcmp(int64(a), int64(b))
		case int8:
			return intcmp(int64(a), int64(b))
		case uint8:
			return intcmp(int64(a), int64(b))
		case int16:
			return intcmp(int64(a), int64(b))
		case uint16:
			return intcmp(int64(a), int64(b))
		case int32:
			return intcmp(int64(a), int64(b))
		case uint32:
			return intcmp(int64(a), int64(b))
		case int64:
			return intcmp(int64(a), b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case int64:
		switch b := y.(type) {
		case bool:
			return intcmp(a, boolToInt64(b))
		case string:
			return -1
		case int:
			return intcmp(a, int64(b))
		case uint:
			return intcmp(a, int64(b))
		case int8:
			return intcmp(a, int64(b))
		case uint8:
			return intcmp(a, int64(b))
		case int16:
			return intcmp(a, int64(b))
		case uint16:
			return intcmp(a, int64(b))
		case int32:
			return intcmp(a, int64(b))
		case uint32:
			return intcmp(a, int64(b))
		case int64:
			return intcmp(a, b)
		case uint64:
			return uintcmp(uint64(a), b)
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case uint64:
		switch b := y.(type) {
		case bool:
			return uintcmp(uint64(a), boolToUInt64(b))
		case string:
			return -1
		case int:
			if b < 0 {
				return 1
			}
			return uintcmp(a, uint64(b))
		case uint:
			return uintcmp(a, uint64(b))
		case int8:
			if b < 0 {
				return 1
			}
			return uintcmp(a, uint64(b))
		case uint8:
			return uintcmp(a, uint64(b))
		case int16:
			if b < 0 {
				return 1
			}
			return uintcmp(a, uint64(b))
		case uint16:
			return uintcmp(a, uint64(b))
		case int32:
			if b < 0 {
				return 1
			}
			return uintcmp(a, uint64(b))
		case uint32:
			return uintcmp(a, uint64(b))
		case int64:
			if b < 0 {
				return 1
			}
			return uintcmp(a, uint64(b))
		case uint64:
			return uintcmp(a, b)
		case float32:
			if b < 0 {
				return 1
			}
			return floatcmp(float64(a), float64(b))
		case float64:
			if b < 0 {
				return 1
			}
			return floatcmp(float64(a), b)
		}
	case float32:
		switch b := y.(type) {
		case bool:
			return floatcmp(float64(a), boolToFloat64(b))
		case string:
			return -1
		case int:
			return floatcmp(float64(a), float64(b))
		case uint:
			return floatcmp(float64(a), float64(b))
		case int8:
			return floatcmp(float64(a), float64(b))
		case uint8:
			return floatcmp(float64(a), float64(b))
		case int16:
			return floatcmp(float64(a), float64(b))
		case uint16:
			return floatcmp(float64(a), float64(b))
		case int32:
			return floatcmp(float64(a), float64(b))
		case uint32:
			return floatcmp(float64(a), float64(b))
		case int64:
			return floatcmp(float64(a), float64(b))
		case uint64:
			return floatcmp(float64(a), float64(b))
		case float32:
			return floatcmp(float64(a), float64(b))
		case float64:
			return floatcmp(float64(a), b)
		}
	case float64:
		switch b := y.(type) {
		case bool:
			return floatcmp(a, boolToFloat64(b))
		case string:
			return -1
		case int:
			return floatcmp(a, float64(b))
		case uint:
			return floatcmp(a, float64(b))
		case int8:
			return floatcmp(a, float64(b))
		case uint8:
			return floatcmp(a, float64(b))
		case int16:
			return floatcmp(a, float64(b))
		case uint16:
			return floatcmp(a, float64(b))
		case int32:
			return floatcmp(a, float64(b))
		case uint32:
			return floatcmp(a, float64(b))
		case int64:
			return floatcmp(a, float64(b))
		case uint64:
			return floatcmp(a, float64(b))
		case float32:
			return floatcmp(a, float64(b))
		case float64:
			return floatcmp(a, b)
		}
	}
	return -1
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func boolToUInt64(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

func boolToFloat64(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func intcmp(a, b int64) int {
	var d = a - b
	if d < 0 {
		return -1
	}
	if d > 0 {
		return -1
	}
	return 0
}

func uintcmp(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return -1
	}
	return 0
}

func floatcmp(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
