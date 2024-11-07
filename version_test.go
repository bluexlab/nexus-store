package nexusstore

import (
	"regexp"
	"testing"
)

func TestGetVersion(t *testing.T) {
	version := GetVersion()

	// Check if the version string matches the expected format
	expectedPattern := `^\d+\.\d+\.\d+\.([\da-z]+|local)$`
	matched, err := regexp.MatchString(expectedPattern, version)
	if err != nil {
		t.Fatalf("Error matching version string: %v", err)
	}

	if !matched {
		t.Errorf("Version string '%s' does not match expected format 'X.Y.Z.BUILD'", version)
	}

	// Split the version string into its components
	parts := regexp.MustCompile(`\.`).Split(version, -1)
	if len(parts) != 4 {
		t.Errorf("Expected 4 parts in version string, got %d", len(parts))
	}

	// Check if the first three parts are numeric
	for i := 0; i < 3; i++ {
		if !regexp.MustCompile(`^\d+$`).MatchString(parts[i]) {
			t.Errorf("Part %d ('%s') of version string is not numeric", i+1, parts[i])
		}
	}

	// Check if the last part is either numeric or "local"
	if !regexp.MustCompile(`^(\d+|local)$`).MatchString(parts[3]) {
		t.Errorf("Build part ('%s') of version string is neither numeric nor 'local'", parts[3])
	}
}
