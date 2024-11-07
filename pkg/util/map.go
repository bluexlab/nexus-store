package util

import "github.com/samber/lo"

// MapSlice map a slice to a slice of another type.
func MapSlice[T any, R any](collection []T, mapFunc func(T) R) []R {
	return lo.Map(collection, func(t T, _ int) R { return mapFunc(t) })
}
