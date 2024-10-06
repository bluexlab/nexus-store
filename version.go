package nexusstore

import (
	"embed"
	"regexp"
	"strings"
)

//go:embed VERSION BUILD
var EmbeddedFS embed.FS

const ServiceName = "nexus-store"

func GetVersion() string {
	// Read version from embedded file
	versionBytes, err := EmbeddedFS.ReadFile("VERSION")
	version := "0.0.0"
	if err == nil {
		version = strings.TrimSpace(string(versionBytes))
	}

	// Read build number from embedded file
	buildBytes, err := EmbeddedFS.ReadFile("BUILD")
	build := "local"
	if err == nil {
		build = strings.TrimSpace(string(buildBytes))

		re := regexp.MustCompile(`\d+`)
		match := re.FindString(build)

		if match != "" {
			build = match
		}
	}

	return version + "." + build
}
