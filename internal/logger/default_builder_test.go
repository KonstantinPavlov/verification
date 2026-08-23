package logger

import (
	"bytes"
	"context"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDefaultBuilder_InitialStateAndFluent(t *testing.T) {
	builder := NewDefaultBuilder()
	if builder == nil {
		t.Fatal("NewDefaultBuilder() returned nil")
	}

	buf := new(bytes.Buffer)

	// Проверяем изменение уровней через цепочку вызовов
	b1 := NewDefaultBuilder().Level(slog.LevelError)
	h1 := b1.Build(buf).(*DefaultHandler)
	if h1.opts.Level.Level() != slog.LevelError {
		t.Errorf("Level() failed: expected ERROR, got %v", h1.opts.Level.Level())
	}

	b2 := NewDefaultBuilder().LevelStr("WARN")
	h2 := b2.Build(buf).(*DefaultHandler)
	if h2.opts.Level.Level() != slog.LevelWarn {
		t.Errorf("LevelStr() failed: expected WARN, got %v", h2.opts.Level.Level())
	}

	// По умолчанию AddSource должен быть равен true
	if !h2.opts.AddSource {
		t.Error("Expected AddSource to be true by default via builder")
	}
}

func TestDefaultBuilder_BuildWithSourceOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	handler := NewDefaultBuilder().Level(slog.LevelInfo).Build(buf)

	var pcs [1]uintptr
	_ = runtime.Callers(1, pcs[:])

	record := slog.NewRecord(time.Now(), slog.LevelInfo, "test source", pcs[0])
	_ = handler.Handle(context.Background(), record)

	result := buf.String()

	// Проверяем, что в лог попало имя текущего файла тестов
	if !strings.Contains(result, "builder_test.go") {
		t.Errorf("Expected source file 'builder_test.go' in output, got: %q", result)
	}
}

func TestTeeHandler_RoutingAndFiltering(t *testing.T) {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)

	h1 := &DefaultHandler{commonHandler: newCommonHandler(buf1, &slog.HandlerOptions{Level: slog.LevelInfo})}
	h2 := &DefaultHandler{commonHandler: newCommonHandler(buf2, &slog.HandlerOptions{Level: slog.LevelError})}

	tee := &teeHandler{handlers: []slog.Handler{h1, h2}}
	ctx := context.Background()

	// Сценарий 1: Лог INFO (h2 должен его пропустить)
	rec1 := slog.NewRecord(time.Now(), slog.LevelInfo, "info-level-msg", 0)
	if !tee.Enabled(ctx, slog.LevelInfo) {
		t.Error("TeeHandler should be enabled for INFO")
	}
	_ = tee.Handle(ctx, rec1)

	if !strings.Contains(buf1.String(), "info-level-msg") {
		t.Errorf("h1 should have logged message, got: %q", buf1.String())
	}
	if buf2.Len() > 0 {
		t.Errorf("h2 (ERROR level) should have skipped info message, got: %q", buf2.String())
	}

	buf1.Reset()
	buf2.Reset()

	// Сценарий 2: Лог ERROR (должен записаться везде)
	rec2 := slog.NewRecord(time.Now(), slog.LevelError, "error-level-msg", 0)
	_ = tee.Handle(ctx, rec2)

	if !strings.Contains(buf1.String(), "error-level-msg") || !strings.Contains(buf2.String(), "error-level-msg") {
		t.Errorf("Both handlers should have logged error message. h1: %q, h2: %q", buf1.String(), buf2.String())
	}
}

func TestTeeHandler_WithAttrsAndGroupPropagation(t *testing.T) {
	buf := new(bytes.Buffer)
	h := &DefaultHandler{commonHandler: newCommonHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})}
	tee := &teeHandler{handlers: []slog.Handler{h}}

	propagatedTee := tee.WithGroup("worker").WithAttrs([]slog.Attr{slog.String("tier", "pro")})

	rec := slog.NewRecord(time.Now(), slog.LevelDebug, "job done", 0)
	_ = propagatedTee.Handle(context.Background(), rec)

	expected := `worker.tier="pro"`
	if !strings.Contains(buf.String(), expected) {
		t.Errorf("TeeHandler failed to propagate methods. Expected %q, got: %q", expected, buf.String())
	}
}
