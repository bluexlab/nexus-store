package dbaccess

import "errors"

// ErrNotFound is returned when a query by ID finds no matching rows.
// For example, canceling a non-existent job returns this error.
var ErrNotFound = errors.New("not found")
