package config

import (
	"log"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitLogger initializes the global zap logger.
// Level is controlled by LOG_LEVEL env var (debug/info/warn/error).
// When ENV is set to "production", uses JSON output; otherwise console.
func InitLogger() *zap.Logger {
	env := os.Getenv("ENV")
	level := parseLogLevel(os.Getenv("LOG_LEVEL"))

	var l *zap.Logger
	var err error
	if env == "production" {
		l, err = zap.NewProduction(zap.IncreaseLevel(level))
	} else {
		l, err = zap.NewDevelopment(zap.IncreaseLevel(level))
	}
	if err != nil {
		log.Fatalf("init logger: %v", err)
	}

	return l
}

func parseLogLevel(s string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		if s != "" {
			log.Printf("unknown LOG_LEVEL=%q, using default", s)
		}
		if os.Getenv("ENV") == "production" {
			return zapcore.InfoLevel
		}
		return zapcore.DebugLevel
	}
}
