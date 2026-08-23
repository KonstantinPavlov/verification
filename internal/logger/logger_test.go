package logger

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestStrLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"Warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"unknown", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if res := StrLevel(tt.input); res.Level() != tt.expected {
				t.Errorf("StrLevel(%q) = %v; want %v", tt.input, res, tt.expected)
			}
		})
	}
}

func TestAttrValueToString(t *testing.T) {
	tests := []struct {
		name     string
		input    slog.Value
		expected string
	}{
		{"bool", slog.BoolValue(true), "true"},
		{"int", slog.Int64Value(42), "42"},
		{"float", slog.Float64Value(3.14), "3.14"},
		{"string", slog.StringValue("hello"), `"hello"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := attrValueToString(tt.input); res != tt.expected {
				t.Errorf("attrValueToString() = %q; want %q", res, tt.expected)
			}
		})
	}
}

func TestGetGoroutineId(t *testing.T) {
	id := getGoroutineId()
	if len(id) == 0 {
		t.Error("Expected non-empty goroutine ID")
	}
	if bytes.Contains(id, []byte(" ")) {
		t.Error("Goroutine ID should not contain spaces")
	}
}

func TestNewLogger(t *testing.T) {
	// 1. Тест инициализации без аргументов
	lDef := New()
	if lDef == nil || lDef.Logger == nil {
		t.Fatal("Logger initialization failed with defaults")
	}

	// 2. Тест с одним кастомным хэндлером
	buf := new(bytes.Buffer)
	h := &DefaultHandler{commonHandler: newCommonHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})}
	lSingle := New(h)

	lSingle.Info("single handler test")
	if !strings.Contains(buf.String(), "single handler test") {
		t.Errorf("Logger with single handler failed to log: %q", buf.String())
	}

	// 3. Тест методов-оберток самого логгера WithAttrs и WithGroup
	lWith := lSingle.WithGroup("api").WithAttrs([]slog.Attr{slog.String("version", "v1")})
	lWith.Info("call")
	if !strings.Contains(buf.String(), `api.version="v1"`) {
		t.Errorf("Logger wrapper WithGroup/WithAttrs failed, output: %q", buf.String())
	}
}
