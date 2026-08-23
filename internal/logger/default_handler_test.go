package logger

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDefaultHandler_Enabled(t *testing.T) {
	opts := &slog.HandlerOptions{Level: slog.LevelWarn}
	handler := &DefaultHandler{commonHandler: newCommonHandler(new(bytes.Buffer), opts)}

	if handler.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("Expected DEBUG to be disabled when handler level is WARN")
	}
	if !handler.Enabled(context.Background(), slog.LevelError) {
		t.Error("Expected ERROR to be enabled when handler level is WARN")
	}
}

func TestDefaultHandler_FormatAndClean(t *testing.T) {
	buf := new(bytes.Buffer)
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	handler := &DefaultHandler{commonHandler: newCommonHandler(buf, opts)}

	// Фиксированное время для стабильного лога
	testTime := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	record := slog.NewRecord(testTime, slog.LevelInfo, "Hello\nWorld\t!", 0)
	record.AddAttrs(slog.String("key", "val"))

	err := handler.Handle(context.Background(), record)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	result := buf.String()

	// Проверяем формат времени и уровня
	if !strings.Contains(result, "2026") {
		t.Errorf("Log output missing correct time format. Got: %q", result)
	}
	if !strings.Contains(result, "[INFO]") {
		t.Errorf("Log output missing level block. Got: %q", result)
	}
	if !strings.Contains(result, `(key="val")|`) {
		t.Errorf("Log output missing MDC/Attributes formatting. Got: %q", result)
	}

	// Проверяем работу cleanPattern (замена переносов строк и табов на пробелы)
	if strings.Contains(result, "\t") {
		t.Errorf("Log contains internal tab characters: %q", result)
	}
	if strings.Count(result, "\n") != 1 {
		t.Errorf("Log should contain exactly one newline character at the end. Got count: %d", strings.Count(result, "\n"))
	}
	if !strings.Contains(result, "Hello World !") {
		t.Errorf("Message cleaning failed to replace internal special characters. Got: %q", result)
	}
}

func TestDefaultHandler_WithAttrsAndGroup(t *testing.T) {
	buf := new(bytes.Buffer)
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	var handler slog.Handler = &DefaultHandler{commonHandler: newCommonHandler(buf, opts)}

	// Цепочка групп и атрибутов
	handler = handler.WithGroup("request").WithAttrs([]slog.Attr{slog.Int("id", 777)})

	record := slog.NewRecord(time.Now(), slog.LevelInfo, "processing", 0)
	record.AddAttrs(slog.String("status", "pending"))

	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	result := buf.String()
	// Проверяем вложенность путей MDC
	expectedMDC := `request.id=777, request.status="pending"`
	if !strings.Contains(result, expectedMDC) {
		t.Errorf("Expected MDC paths %q not found in result: %q", expectedMDC, result)
	}
}
