package util

import "time"

func SecondsAsDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}
