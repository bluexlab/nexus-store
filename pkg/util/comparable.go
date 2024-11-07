package util

import "golang.org/x/exp/constraints"

// Clamp constrains a value within a specified range.
//
// It takes three arguments of type T, where T is any ordered type (numbers, strings, etc.):
//   - value: The input value to be clamped.
//   - min: The lower bound of the range.
//   - max: The upper bound of the range.
//
// The function returns:
//   - If value is less than min, it returns min.
//   - If value is greater than max, it returns max.
//   - Otherwise, it returns value unchanged.
//
// This function is useful for ensuring a value stays within a specific range,
// which can be helpful in various scenarios such as input validation,
// preventing out-of-bounds errors, or implementing game logic.
//
// Example usage:
//
//	result := Clamp(10, 0, 5)  // Returns 5
//	result := Clamp(-1, 0, 5)  // Returns 0
//	result := Clamp(3, 0, 5)   // Returns 3
//
// Note: This function assumes that min <= max. If this condition is not met,
// the behavior is undefined and may produce unexpected results.
func Clamp[T constraints.Ordered](value, min, max T) T {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
