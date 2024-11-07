package util

import (
	"testing"
)

func TestClamp(t *testing.T) {
	tests := []struct {
		name     string
		value    int
		min      int
		max      int
		expected int
	}{
		{"Value within range", 3, 0, 5, 3},
		{"Value below min", -1, 0, 5, 0},
		{"Value above max", 10, 0, 5, 5},
		{"Value equal to min", 0, 0, 5, 0},
		{"Value equal to max", 5, 0, 5, 5},
		{"Min equal to max", 3, 5, 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Clamp(tt.value, tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("Clamp(%d, %d, %d) = %d; want %d", tt.value, tt.min, tt.max, result, tt.expected)
			}
		})
	}
}

func TestClampFloat(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		min      float64
		max      float64
		expected float64
	}{
		{"Float value within range", 3.5, 0.0, 5.0, 3.5},
		{"Float value below min", -1.5, 0.0, 5.0, 0.0},
		{"Float value above max", 10.5, 0.0, 5.0, 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Clamp(tt.value, tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("Clamp(%f, %f, %f) = %f; want %f", tt.value, tt.min, tt.max, result, tt.expected)
			}
		})
	}
}

func TestClampString(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		min      string
		max      string
		expected string
	}{
		{"String value within range", "b", "a", "c", "b"},
		{"String value below min", "a", "b", "d", "b"},
		{"String value above max", "e", "b", "d", "d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Clamp(tt.value, tt.min, tt.max)
			if result != tt.expected {
				t.Errorf("Clamp(%q, %q, %q) = %q; want %q", tt.value, tt.min, tt.max, result, tt.expected)
			}
		})
	}
}
