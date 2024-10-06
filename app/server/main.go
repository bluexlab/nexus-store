package main

import (
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
	nexusstore "gitlab.com/navyx/nexus/nexus-store"
)

func main() {
	// Load environment variables from .env file if exists
	if _, err := os.Stat(".env"); err == nil {
		godotenv.Load(".env")
	}

	logger := initLogger()
	app := &App{logger}
	app.Run()
}

func initLogger() *slog.Logger {
	// Get log level from environment variable, default to "info"
	logLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level := slog.LevelDebug
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	// Create Logger based on environment
	var logger *slog.Logger
	if os.Getenv("DEV") != "" {
		handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
		logger = slog.New(handler)
	} else {
		opts := slog.HandlerOptions{
			AddSource:   true,
			Level:       level,
			ReplaceAttr: unixTimestampHandler,
		}
		handler := slog.NewJSONHandler(os.Stdout, &opts)
		logger = slog.New(handler).With(
			"service", nexusstore.ServiceName,
			"version", nexusstore.GetVersion(),
		)
	}

	// Set as default logger
	slog.SetDefault(logger)

	return logger
}

func unixTimestampHandler(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Int64("ts", a.Value.Time().UnixNano()/1e6) // millisecond precision
	}
	return a
}
